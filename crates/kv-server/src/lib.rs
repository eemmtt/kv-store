use std::{os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd}, path::Path, time::Duration};
use nix::{errno::Errno, fcntl::{OFlag, open}, libc::pthread_mutex_t, sys::{socket::{AddressFamily, Backlog, SockFlag, SockType, UnixAddr, accept, bind, listen, socket}, stat::Mode}, unistd::{Whence, close, lseek, unlink}};
use kv_shared::{io::{DUR_SIZE, KEY_SIZE, KVKey, KVLogItem, KVStore, KVValue, SEC_SIZE, VAL_SIZE}, ringbuffer::FdRingBuffer, semaphores::{kv_mutex_init, kv_mutex_lock, kv_mutex_unlock}, syncdindex::SyncdIndex};

pub fn kv_load_log(store_path: &Path) -> Result<OwnedFd,()>{
    let log_path = store_path.join("kvlog");
    let fd = match open(
        &log_path, 
        OFlag::O_RDWR | OFlag::O_CREAT, 
        Mode::S_IRWXU,
    ){
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("kv_load_log::open() failed with: {}", e);
            return Err(());
        }
    };

    Ok(fd)
}

pub fn kv_load_index(store_path: &Path) -> Result<SyncdIndex, ()>{
    let index_path = store_path.join("kvindex");
    let index = match SyncdIndex::from_file(&index_path){
        Ok(si) => si,
        Err(Errno::ENOENT) => {
            eprintln!("kv_load_index couldn't find an index file so it created a blank index instead");
            SyncdIndex::new()
        }
        Err(e) => {
            eprintln!("SyncdIndex::from_file() failed with {}", e);
            return Err(());
        }
    };
    Ok(index)
}

/// Close log and persist index to file
pub fn kv_store_shutdown(store: KVStore, store_path: &Path) -> Result<(), Errno>{
    
    /* compact log */
    let compacted_store = kv_store_compact(store, store_path).unwrap();
    
    /* persist index */
    let index_path = store_path.join("kvindex");
    compacted_store.index.to_file(&index_path).unwrap();

    /* close log */
    let _ = close(compacted_store.fd).unwrap();

    Ok(())
}

/// Compact log file to most recent undeleted data
pub fn kv_store_compact(log: KVStore, store_path: &Path) -> Result<KVStore, Errno>{
    /* init new log and index */
    let new_log_path = store_path.join("newlog");
    let mut new_index = SyncdIndex::new();
    let new_log_fd = open(
        &new_log_path, 
        OFlag::O_RDWR
        | OFlag::O_CREAT,
        Mode::S_IRWXU
    ).expect("kv_store_compact open fail");

    /* loop over old index, write all entries to new log and index */
    for (key, item) in log.index.map.iter(){
        /* read old val into buf */
        let _old_offset = nix::unistd::lseek(&log.fd, item.file_offset as i64, Whence::SeekSet).unwrap();
        let mut buf = vec![0u8;item.data_size];
        let mut nbytes_read = 0;
        loop {
            match nix::unistd::read(&log.fd, &mut buf[nbytes_read..]){
                Ok(0) => break,
                Ok(n) => nbytes_read += n,
                Err(e) => return Err(e),
            }
        }

        /* write old val to end of new log */
        let new_offset = nix::unistd::lseek(&new_log_fd, 0, Whence::SeekEnd).unwrap();
        let mut nbytes_written = 0;
        while nbytes_written < nbytes_read {
            match nix::unistd::write(&new_log_fd, &buf[nbytes_written..nbytes_read]){
                Ok(n) => nbytes_written += n,
                Err(e) => return Err(e),
            }
        }

        /* insert new item to new index */
        let new_item = KVLogItem {
            file_offset: new_offset as isize,
            data_size: nbytes_written,
            time_added: item.time_added,
        };
        if new_index.map.insert(*key, new_item).is_some(){
            eprintln!("something weird happened with new index");
            return Err(Errno::EALREADY);
        }
    }

    /* rename old and new logs */
    let log_old_path_init = store_path.join("kvlog");
    let log_old_path_fin = store_path.join("kvlog_old");
    let log_new_path_init = store_path.join("newlog");
    let log_new_path_fin = store_path.join("kvlog");
    nix::fcntl::renameat(
        nix::fcntl::AT_FDCWD, 
        &log_old_path_init, 
        nix::fcntl::AT_FDCWD, 
        &log_old_path_fin
    ).expect("failed rename old log");
    nix::fcntl::renameat(
        nix::fcntl::AT_FDCWD, 
        &log_new_path_init, 
        nix::fcntl::AT_FDCWD, 
        &log_new_path_fin
    ).expect("failed rename new log");

    /* cleanup old log */
    nix::unistd::close(log.fd).unwrap();

    Ok(KVStore{
        fd: new_log_fd,
        index: new_index,
        mtx: kv_mutex_init().unwrap(),
    })
}

/// Get value from log
pub fn kv_store_get(store: &mut KVStore, key: &KVKey) -> Result<Option<KVValue>, Errno>{
    let logitem = match store.index.si_get(*key){
        Some(li) => li,
        None => {
            return Ok(None);
        }
    };

    /* seek to data location and read data_size bytes */
    kv_mutex_lock(&mut store.mtx).unwrap();
    lseek(&store.fd, logitem.file_offset as i64, Whence::SeekSet).unwrap();
    /* Entries are formatted: [key|set_time|deletion_time|value] */
    let mut buf = [0u8; KEY_SIZE + DUR_SIZE + DUR_SIZE + VAL_SIZE]; 
    let mut bytes_read = 0;
    while bytes_read < logitem.data_size {
        match nix::unistd::read(&store.fd, &mut buf[bytes_read..]) {
            Ok(0) => break,
            Ok(n) => bytes_read += n,
            Err(e) => {
                return Err(e);
            }
        }
    }
    kv_mutex_unlock(&mut store.mtx).unwrap();

    /* check if entry was deleted */
    let mut del_time = [0u8;DUR_SIZE];
    del_time.copy_from_slice(&buf[KEY_SIZE + DUR_SIZE..KEY_SIZE + DUR_SIZE + DUR_SIZE]);
    if del_time.iter().any(|&x| x != 0){
        return Err(Errno::ENOENT);
    }

    let val = KVValue::from_bytes(&buf[KEY_SIZE + DUR_SIZE + DUR_SIZE..]).unwrap();
    Ok(Some(val))

}

/// Set key value pair in log
pub fn kv_store_set(store: &mut KVStore, key: &KVKey, val: &KVValue, set_time: Duration) -> Result<(), Errno>{
    let logitem = store.index.si_get(*key);

    kv_mutex_lock(&mut store.mtx).unwrap();
    if logitem.is_some(){
        /* there is an existing value for this key. Check that the new value is fresh. If not, return early */
        if logitem.unwrap().time_added > set_time {
            kv_mutex_unlock(&mut store.mtx).unwrap();
            return Err(Errno::ESTALE);
        }

        /* update previous log value with deletion time */
        let deletion_time_offset = logitem.unwrap().file_offset as usize + KEY_SIZE + DUR_SIZE;
        lseek(&store.fd, deletion_time_offset as i64, Whence::SeekSet).unwrap();
        let mut del_time = [0u8;DUR_SIZE];
        del_time[..SEC_SIZE].copy_from_slice(&set_time.as_secs().to_le_bytes());
        del_time[SEC_SIZE..].copy_from_slice(&set_time.subsec_nanos().to_le_bytes());
        let mut bytes_written = 0;
        while bytes_written < del_time.len() {
            match nix::unistd::write(&store.fd, &del_time[bytes_written..]){
                Ok(n) => bytes_written += n,
                Err(e) => return Err(e),
            }
        }

    }

    /* write new entry to log. Entries are formatted: [key|set_time|deletion_time|value] */
    let mut entry = [0u8;KEY_SIZE + DUR_SIZE + DUR_SIZE + VAL_SIZE];
    entry[..KEY_SIZE].copy_from_slice(&key.to_bytes());
    entry[KEY_SIZE..KEY_SIZE + SEC_SIZE].copy_from_slice(&set_time.as_secs().to_le_bytes());
    entry[KEY_SIZE + SEC_SIZE..KEY_SIZE + DUR_SIZE].copy_from_slice(&set_time.subsec_nanos().to_le_bytes());
    /* skip over a DUR_SIZE for deletion_time because array already initialized to 0's */
    entry[KEY_SIZE + DUR_SIZE + DUR_SIZE..].copy_from_slice(&val.to_bytes());

    let new_offset = lseek(&store.fd, 0, Whence::SeekEnd).unwrap();
    let mut bytes_written = 0;
    while bytes_written < entry.len() {
        match nix::unistd::write(&store.fd, &entry[bytes_written..]){
            Ok(n) => bytes_written += n,
            Err(e) => return Err(e),
        }
    }
    kv_mutex_unlock(&mut store.mtx).unwrap();

    /* add logitem to index */
    let new_logitem = KVLogItem{
        file_offset: new_offset as isize,
        data_size: entry.len(),
        time_added: set_time,
    };
    store.index.si_insert(*key, new_logitem).unwrap();
    Ok(())

}

/// Delete key value pair from log
pub fn kv_store_delete(store: &mut KVStore, key: &KVKey, del_time: Duration) -> Result<(), Errno>{
    let logitem = store.index.si_get(*key);
    if logitem.is_none(){
        /* nothing to delete! */
        return Err(Errno::ENOENT);
    }
    if logitem.is_some(){
        /* check that we're not deleting a key updated after the deletion was sent */
        if logitem.unwrap().time_added > del_time {
            return Err(Errno::ESTALE);
        }

        /* update log entry with deletion time */
        /* entries are formatted: [key|set_time|deletion_time|value] */
        kv_mutex_lock(&mut store.mtx).unwrap();
        let deletion_time_offset = logitem.unwrap().file_offset as usize + KEY_SIZE + DUR_SIZE;
        lseek(&store.fd, deletion_time_offset as i64, Whence::SeekSet).unwrap();

        let mut del_time_bytes = [0u8;DUR_SIZE];
        del_time_bytes[..SEC_SIZE].copy_from_slice(&del_time.as_secs().to_le_bytes());
        del_time_bytes[SEC_SIZE..].copy_from_slice(&del_time.subsec_nanos().to_le_bytes());

        let mut bytes_written = 0;
        while bytes_written < del_time_bytes.len() {
            match nix::unistd::write(&store.fd, &del_time_bytes[bytes_written..]){
                Ok(n) => bytes_written += n,
                Err(e) => {
                    eprintln!("write failed: {}", e);
                    return Err(e);
                },
            }
        }
        kv_mutex_unlock(&mut store.mtx).unwrap();

        /* remove logitem from index */
        store.index.si_delete(*key).expect("kv_store_delete wasn't able to remove the key from the index");
    }
    
    Ok(())
}


/// Open unix tcp socket, bind and listen
pub fn open_socket(path: &Path) -> Result<OwnedFd, Errno>{
    let socket_addr = UnixAddr::new(path).expect("open_socket: UnixAddr failed");

    let _ = match unlink(path){
        Ok(_) => (),
        Err(Errno::ENOENT) => (), /* .sock already exists, continue */
        Err(e) => {
            eprintln!("open_socket: unlink socket_path: {}", e);
            return Err(e);
        }
    };

    let sockfd = socket(
        AddressFamily::Unix, 
        SockType::Stream,
        SockFlag::empty(),
        None,
    ).expect("open_socket: socket failed");

    bind(sockfd.as_raw_fd(), &socket_addr).expect("open_socket: bind failed");
    listen(&sockfd, Backlog::MAXCONN).expect("open_socket: listen failed");

    Ok(sockfd)
}

pub fn accept_connection(socket_fd: &OwnedFd, rbuf: &mut FdRingBuffer) -> Result<(), Errno>{
    /* todo: write connectionfd into a buffer */

    let connfd_raw: RawFd = accept(socket_fd.as_raw_fd()).expect("accept failed");
    let connfd = unsafe { OwnedFd::from_raw_fd(connfd_raw) };
    rbuf.put(connfd).expect("FdRingBuffer full or bad put");
    return Ok(());
}

pub mod polling {
    use std::os::fd::OwnedFd;

    use nix::{errno::Errno, sys::epoll::{Epoll, EpollEvent, EpollFlags}};

    pub enum PollInterests {
        ListeningSocket = 0,
        TerminalInput = 1,
        SIGINT = 2,
    }

    pub fn kv_epoll_add(epoll: &Epoll, fd: &OwnedFd, flags: EpollFlags, interest: PollInterests) -> Result<(), Errno>{
        epoll.add(fd, EpollEvent::new(flags, interest as u64))?;
        Ok(())
    }
}

pub mod threading {
    use std::ffi::c_void;
    use nix::libc::{pthread_t, pthread_create, pthread_self, pthread_detach};
    use nix::errno::Errno;    
    
    /// Wrapper for libc::pthread_create, takes no attributes
    pub fn kv_pthread_create(
        thread: *mut pthread_t,  
        thread_fn: extern "C" fn(*mut c_void) -> *mut c_void, 
        fn_arg: *mut c_void 
    ) -> Result<(), Errno>{
        
        let res = unsafe { 
            pthread_create(thread, std::ptr::null(), thread_fn, fn_arg) 
        };
        if res == 0 {
            Ok(())
        } else {
            let e = Errno::from_raw(res);
            eprintln!("pthread_create: {}", e);
            Err(e)
        }
    }
    
    /// Wrapper for libc::pthread_detach, detaches the calling thread
    pub fn kv_pthread_detach() -> Result<(), Errno>{
    
        let tid = unsafe { pthread_self() };
        let res = unsafe { pthread_detach(tid) };
        if res == 0 {
            Ok(())
        } else {
            let e = Errno::from_raw(res);
            eprintln!("pthread_detach: {}", e);
            Err(e)
        }
    }
}

pub mod worker{
    use std::{ffi::c_void, os::fd::OwnedFd};

    use kv_shared::{io::{KEY_SIZE, KVConnection, KVKey, KVMsg, KVMsgType, KVStore, KVValue, VAL_SIZE}, ringbuffer::FdRingBuffer};
    use nix::errno::Errno;
    
    use crate::{kv_store_delete, kv_store_get, kv_store_set, threading::kv_pthread_detach};
    
    /// Data passed as arg to worker_thread
    pub struct WorkerData<'a>{
        pub id: u64,
        pub rbuf: &'a mut FdRingBuffer,
        pub log: &'a mut KVStore,
    }
    
    /// start routine for worker threads
    pub extern "C" fn worker_thread(arg: *mut c_void) -> *mut c_void{
        kv_pthread_detach().unwrap();
        let data = unsafe { Box::from_raw(arg as *mut WorkerData)};
        println!("Hello from worker thread #{}!", data.id);

        /* lock with timeout, check for shutdown flag */
        loop {
            let fd = match data.rbuf.get(){
                Some(fd) => fd,
                None => {
                    continue;
                }
            };
            handle_connection(fd, data.log,data.id).expect("oops at handle_connection");
        }
    
        std::ptr::null_mut()
    }

    fn handle_connection(fd: OwnedFd, log: &mut KVStore, workerid: u64) -> Result<(), Errno>{
    
        let mut connection = KVConnection{
            fd: fd,
            mtu: 1024,
        };
    
        #[allow(unused)]
        'receive_commands: loop {
            let msg = match connection.recv_kvmsg(){
                Ok(msg) => msg,
                Err(Errno::ECONNRESET) => {
                    println!("worker #{}: client disconnected", workerid);
                    break;
                },
                Err(e) => {
                    eprintln!("handle_connection recv_all: error {}", e);
                    return Err(e);
                }
            };
        
            match msg.msgtype {
                KVMsgType::Get => {
                    println!("worker #{}: GET start", workerid);
                    let key = KVKey::from_bytes(&msg.data[..KEY_SIZE]).unwrap();
                    let return_val = match kv_store_get(log, &key){
                        Ok(None) => KVValue::new("").unwrap(),
                        Ok(v) => v.unwrap(),
                        Err(Errno::ENOENT) => KVValue::new("").unwrap(),
                        Err(e) => {
                            eprint!("kv_log_get err: {}", e);
                            return Err(e);
                        }
                    };
                    let msg = KVMsg::new(KVMsgType::GetReturn, key, return_val);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: GET end", workerid);
                },
                KVMsgType::Set => {                     
                    println!("worker #{}: SET start", workerid);
                    let key = KVKey::from_bytes(&msg.data[..KEY_SIZE]).unwrap();
                    let val = KVValue::from_bytes(&msg.data[KEY_SIZE..KEY_SIZE + VAL_SIZE]).unwrap(); 
                    let result = match kv_store_set(log, &key, &val, msg.sendtime){
                        Ok(_) => KVValue::new("ok").unwrap(),
                        Err(_) => KVValue::new("set failed").unwrap(),
                    };

                    let msg = KVMsg::new(KVMsgType::SetReturn, key, result);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: SET end", workerid);
                },
                KVMsgType::Delete => {
                    println!("worker #{}: DEL start", workerid);
                    let key = KVKey::from_bytes(&msg.data[..KEY_SIZE]).unwrap();
                    let result = match kv_store_delete(log, &key, msg.sendtime){
                        Ok(_) => KVValue::new("ok").unwrap(),
                        Err(Errno::ENONET) => KVValue::new("key not found").unwrap(),
                        Err(Errno::ESTALE) => KVValue::new("delete failed. tried to delete old value").unwrap(),
                        Err(e) => {
                            eprintln!("worker #{}: DEL failed with {}", workerid, e);
                            KVValue::new("delete failed").unwrap()
                        },
                    };
                    
                    let msg = KVMsg::new(KVMsgType::DeleteReturn, key, result);
                    connection.send_kvmsg(msg).unwrap();
                    println!("worker #{}: DEL end", workerid);
                },
                _ => {
                    println!("worker #{}: received unknown msg type", workerid);
                }
            }
        }
    
        Ok(())
    }
}

pub mod signaling{
    use std::{ffi::c_void, os::fd::{RawFd}};
    use nix::libc::{ c_int, write};
    use nix::sys::signal::{Signal};

    pub static mut PIPE_WRITE_FD: Option<RawFd> = None;

    /* this could probably just be handle_sigint */
    pub extern "C" fn handle_signal(signal: c_int) {
        let signal = Signal::try_from(signal).unwrap();

        /* write one bit to self-pipe */
        if signal == Signal::SIGINT {
            unsafe {
                match PIPE_WRITE_FD {
                    Some(fd) => {
                        let mut nbytes: isize = 0;
                        while nbytes == 0 {
                            nbytes = write(fd, &1u8 as *const u8 as *const c_void, 1);
                        }                        
                    },
                    None => {}
                }
            }
        }
    }
}