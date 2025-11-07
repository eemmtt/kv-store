
pub mod io {
    use std::{fmt, os::fd::{AsRawFd, OwnedFd}, path::Path, str::from_utf8, time::{Duration, SystemTime, UNIX_EPOCH}};
    use nix::{errno::Errno, libc::{pthread_mutex_t, size_t}, sys::socket::{MsgFlags, UnixAddr, recv, send}};
    use crate::syncdindex::SyncdIndex;

    pub const USIZE_SIZE: usize = std::mem::size_of::<usize>();
    pub const KEY_SIZE: usize = 256;
    pub const VAL_SIZE: usize = 2048;
    pub const MSG_SIZE: usize = size_of::<u32>() + size_of::<u64>() + size_of::<u32>() + KEY_SIZE + VAL_SIZE;

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    pub struct KVKey {
        len: usize,
        data: [u8; KEY_SIZE - USIZE_SIZE], /* fits in 256 bytes */
    }

    impl KVKey {
        const DATA_SIZE: usize = KEY_SIZE - USIZE_SIZE;

        pub fn new(s: &str) -> Result<Self, ()> {
            if s.len() > Self::DATA_SIZE {
                return Err(());
            }

            let mut data = [0u8; Self::DATA_SIZE];
            data[..s.len()].copy_from_slice(s.as_bytes());
            Ok(Self { 
                data: data, 
                len: s.len() 
            })
        }

        pub fn as_str(&self) -> &str {
            std::str::from_utf8(&self.data[..self.len]).unwrap()
        }

        pub fn to_bytes(&self) -> [u8;KEY_SIZE] {
            let mut bytes = [0u8; KEY_SIZE];
            bytes[..USIZE_SIZE].copy_from_slice(&self.len.to_le_bytes());
            bytes[USIZE_SIZE..].copy_from_slice(&self.data);
            bytes
        }

        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            if bytes.len() != KEY_SIZE {
                eprintln!("KVKey::from_bytes expected {} bytes but got {}.", KEY_SIZE, bytes.len());
                return Err(());
            }
            let len = usize::from_le_bytes(bytes[..USIZE_SIZE].try_into().unwrap());
            let data: [u8;Self::DATA_SIZE] = bytes[USIZE_SIZE..KEY_SIZE].try_into().unwrap();
            Ok(Self { data: data, len: len })
        }
    }

    pub struct KVValue {
        len: usize,
        data: [u8; VAL_SIZE - USIZE_SIZE], /* fits in 2048 bytes */
    }

    impl KVValue {
        const DATA_SIZE: usize = VAL_SIZE - USIZE_SIZE;

        pub fn new(s: &str) -> Result<Self, ()>{
            if s.len() > Self::DATA_SIZE {
                return Err(());
            }

            let mut data = [0u8; Self::DATA_SIZE];
            data[..s.len()].copy_from_slice(s.as_bytes());
            Ok(Self { 
                len: s.len(),
                data: data, 
            })
        }

        pub fn to_bytes(&self) -> [u8;VAL_SIZE]{
            let mut bytes= [0u8; VAL_SIZE];
            bytes[..USIZE_SIZE].copy_from_slice(&self.len.to_le_bytes());
            bytes[USIZE_SIZE..].copy_from_slice(&self.data);
            bytes
        }

        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            if bytes.len() != VAL_SIZE {
                eprintln!("KVVal::from_bytes expected {} bytes but got {}.", VAL_SIZE, bytes.len());
                return Err(());
            }
            let data_len = usize::from_le_bytes(bytes[..USIZE_SIZE].try_into().unwrap());
            let data: [u8; Self::DATA_SIZE] = bytes[USIZE_SIZE..VAL_SIZE].try_into().unwrap();
            
            Ok(Self { 
                len: data_len,
                data,
            })
        }

        pub fn as_str(&self) -> &str {
            std::str::from_utf8(&self.data[..self.len]).unwrap()
        }

    }

    #[derive(Copy, Clone)]
    #[repr(u32)]
    pub enum KVMsgType {
        Get = 0,
        Set = 1,
        Delete = 2,
        GetReturn = 3,
        SetReturn = 4,
        DeleteReturn = 5,
    }

    impl KVMsgType{
        /* convert u32 to KVMsgType */
        fn from_u32(val: u32) -> Result<Self, ()>{
            match val {
                0 => Ok(KVMsgType::Get),
                1 => Ok(KVMsgType::Set),
                2 => Ok(KVMsgType::Delete),
                3 => Ok(KVMsgType::GetReturn),
                4 => Ok(KVMsgType::SetReturn),
                5 => Ok(KVMsgType::DeleteReturn),
                _ => Err(()),
            }
        }
    }
    
    pub struct KVMsg{
        pub msgtype: KVMsgType,
        pub sendtime: Duration, 
        pub data: [u8;KEY_SIZE + VAL_SIZE],
    } 
    
    impl KVMsg{
        const DATA_SIZE: usize = KEY_SIZE + VAL_SIZE;

        pub fn new(msgtype: KVMsgType, key: KVKey, val: KVValue) -> Self{
            let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap();

            let mut data = [0u8;KEY_SIZE + VAL_SIZE];
            data[..KEY_SIZE].copy_from_slice(&key.to_bytes());
            data[KEY_SIZE..].copy_from_slice(&val.to_bytes());
            Self {
                msgtype: msgtype, 
                sendtime: t,
                data,
            }
        }
    
        pub fn to_bytes(&self) -> [u8;MSG_SIZE] {
            let mut bytes = [0u8;MSG_SIZE];
            let sec_offset = size_of::<u32>();
            let nan_offset = sec_offset + size_of::<u64>();
            let msg_offset = nan_offset + size_of::<u32>();

            bytes[..size_of::<u32>()].copy_from_slice(&(self.msgtype as u32).to_le_bytes());
            bytes[sec_offset..nan_offset].copy_from_slice(&self.sendtime.as_secs().to_le_bytes());
            bytes[nan_offset..msg_offset].copy_from_slice(&self.sendtime.subsec_nanos().to_le_bytes());
            bytes[msg_offset..].copy_from_slice(&self.data);
            bytes
        }
        
        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            if bytes.len() != MSG_SIZE {
                eprintln!("KVMsg::from_bytes expected {} bytes but got {}.", MSG_SIZE, bytes.len());
                return Err(());
            }
            
            let msgtype = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let secs = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
            let nanos = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
            let data: [u8; Self::DATA_SIZE] = bytes[16..].try_into().unwrap();
            
            Ok(KVMsg { 
                msgtype: KVMsgType::from_u32(msgtype).unwrap(), 
                sendtime: Duration::new(secs, nanos), 
                data,
            })
            
        }
    } 

    pub struct KVConnection{
        pub fd: OwnedFd,
        pub mtu: size_t,
    }
    
    impl KVConnection{
        
        /// Ensures full send of KVMsg over KVConnection
        pub fn send_kvmsg(&mut self, msg: KVMsg) -> Result<(), Errno>{
            
            let msg_bytes = msg.to_bytes();
            
            /* send msg length over */
            let msg_len = msg_bytes.len();
            let len_buf = msg_len.to_be_bytes() as [u8; 8];
            let mut nbytes_len_sent: usize = 0;
            while nbytes_len_sent < 8 {
                match send(self.fd.as_raw_fd(), &len_buf[nbytes_len_sent..], MsgFlags::empty()){
                    Ok(n) => nbytes_len_sent += n,
                    Err(e) => {
                        eprintln!("io::send_kvmsg send msg_len error: {}", e);
                        return Err(e);
                    }
                }
            }
        
            /* send msg */
            let mut nbytes_msg_sent: usize = 0;
            while nbytes_msg_sent < msg_len {
                 match send(self.fd.as_raw_fd(), &msg_bytes[nbytes_msg_sent..], MsgFlags::empty()){
                    Ok(n) => nbytes_msg_sent += n,
                    Err(e) => {
                        eprintln!("io::send_kvmsg send msg_bytes error: {}", e);
                        return Err(e);
                    }
                }
            }
        
            Ok(())
        }

        /// Ensures full recv of KVMsg over KVConnection
        pub fn recv_kvmsg(&mut self) -> Result<KVMsg, Errno>{
        
            /* receive msg length */ 
            let mut len_buf= [0u8;8]; /* expecting usize */
            let mut nbytes_len_recvd: usize = 0;
            while nbytes_len_recvd < len_buf.len() {
                match recv(self.fd.as_raw_fd(), &mut len_buf[nbytes_len_recvd..], MsgFlags::empty()){
                    Ok(0) => return Err(Errno::ECONNRESET),
                    Ok(n) => nbytes_len_recvd += n,
                    Err(e) => {
                        eprintln!("io::recv_kvmsg recv msg_len error: {}", e);
                        return Err(e);
                    }
                }
            }
            let msg_len = u64::from_be_bytes(len_buf) as usize;
            
            /* receive msg */
            let mut buf = vec![0u8; msg_len];
            let mut nbytes_recvd: usize = 0;
            while nbytes_recvd < msg_len {
                match recv(self.fd.as_raw_fd(), &mut buf[nbytes_recvd..], MsgFlags::empty()){
                    Ok(0) => return Err(Errno::ECONNRESET),
                    Ok(n) => nbytes_recvd += n,
                    Err(e) => {
                        eprintln!("io::recv_all recv msg[len] error: {}", e);
                        return Err(e);
                    }
                }
            }
        
            let result = KVMsg::from_bytes(&buf).unwrap();
            Ok(result)
        }
        
    }

    pub struct KVLogItem {
        pub file_offset: isize,
        pub data_size: usize,
        pub time_added: Duration
    }

    impl KVLogItem {
        pub fn to_bytes(&self) -> Vec<u8> {
            let mut bytes: Vec<u8> = Vec::new();
            bytes.extend(&(self.file_offset as i64).to_le_bytes());
            bytes.extend(&(self.data_size as u64).to_le_bytes());
            bytes.extend(&(self.time_added.as_secs()).to_le_bytes());
            bytes.extend(&(self.time_added.subsec_nanos()).to_le_bytes());
            bytes
        }

        pub fn from_bytes(bytes: &[u8]) -> Result<Self, ()> {
            if bytes.len() != 28 {
                return Err(());
            }

            let file_offset = i64::from_le_bytes(bytes[0..8].try_into().unwrap()) as isize;
            let data_size = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
            let dur_sec = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
            let dur_subsec_nanos = u32::from_le_bytes(bytes[24..].try_into().unwrap());

            Ok(Self {
                file_offset,
                data_size,
                time_added: Duration::new(dur_sec, dur_subsec_nanos),
            })
        }
    }

    impl fmt::Display for KVLogItem {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "LogItem(offset: {}, size: {}, time: {:?})",
                self.file_offset,
                self.data_size,
                self.time_added
            )
        }
    }

    pub struct KVStore{
        pub fd: OwnedFd,
        pub index: SyncdIndex,
        pub mtx: pthread_mutex_t,
    }
}


pub mod ringbuffer {
    use std::os::fd::{OwnedFd};
    use nix::libc::{pthread_mutex_t, sem_t};

    use crate::semaphores::{kv_mutex_init, kv_mutex_lock, kv_mutex_unlock, kv_sem_init, kv_sem_post, kv_sem_wait};

    /* todo: implement ring buffer */
    const RING_BUFFER_SIZE: usize = 4096 / std::mem::size_of::<Option<OwnedFd>>();
    pub struct FdRingBuffer {
        buf: Vec<Option<OwnedFd>>,
        head: usize,
        tail: usize,
        mask: usize,
        mtx: pthread_mutex_t,
        items: sem_t,
        spaces: sem_t,
    }

    impl FdRingBuffer {

        /// Init buffer
        pub fn init()-> Self{
            let mut newbuf: Vec<Option<OwnedFd>> = vec![];
            for _ in 0..RING_BUFFER_SIZE {
                newbuf.push(None);
            }
            
            let mtx = kv_mutex_init().unwrap();
            let items = kv_sem_init(0).unwrap();
            let spaces = kv_sem_init(RING_BUFFER_SIZE).unwrap();

            Self {
                buf: newbuf,
                head: 0,
                tail: 0,
                mask: 0xFFF,
                mtx: mtx,
                items: items,
                spaces: spaces,
            }
        }
    
        /// Put an fd in the buffer, blocking
        pub fn put(&mut self, fd: OwnedFd) -> Result<(), OwnedFd>{
            kv_sem_wait(&mut self.spaces).unwrap();
            kv_mutex_lock(&mut self.mtx).unwrap();

            let index = self.head & self.mask;
            self.buf[index] = Some(fd);
            self.head += 1;

            kv_mutex_unlock(&mut self.mtx).unwrap();
            kv_sem_post(&mut self.items).unwrap();
            
            Ok(())
        }   

        // Get an fd from the buffer, blocking
        pub fn get(&mut self) -> Option<OwnedFd>{
            kv_sem_wait(&mut self.items).unwrap();
            kv_mutex_lock(&mut self.mtx).unwrap();

            let index = self.tail & self.mask;
            let fd = self.buf[index].take()?;
            self.tail += 1;

            kv_mutex_unlock(&mut self.mtx).unwrap();
            kv_sem_post(&mut self.spaces).unwrap();

            Some(fd)
        }
    }


    
}

pub mod semaphores{
    use nix::{errno::Errno, libc::{pthread_mutex_init, pthread_mutex_lock, pthread_mutex_t, pthread_mutex_unlock, sem_init, sem_post, sem_t, sem_wait}};


    /// Wrapper for libc::phread_mutex_init()
    pub fn kv_mutex_init() -> Result<pthread_mutex_t, Errno> {
        let mut mtx: pthread_mutex_t = unsafe { std::mem::zeroed() };
        let res = unsafe {pthread_mutex_init(&mut mtx, std::ptr::null())};
        if res != 0 {
            return Err(Errno::from_raw(res));
        } 
        Ok(mtx)
    }
    
    /// Wrapper for libc::pthread_mutex_lock()
    pub fn kv_mutex_lock(mtx: &mut pthread_mutex_t) -> Result<(), Errno> {
        let res = unsafe {pthread_mutex_lock(mtx)};
        if res != 0 {
            return Err(Errno::from_raw(res));
        } 
        Ok(())
    }
    
    /// Wrapper for libc::pthread_mutex_unlock()
    pub fn kv_mutex_unlock(mtx: &mut pthread_mutex_t) -> Result<(), Errno> {
        let res = unsafe {pthread_mutex_unlock(mtx)};
        if res != 0 {
            return Err(Errno::from_raw(res));
        } 
        Ok(())
    }
    
    /// Wrapper for libc::sem_init()
    pub fn kv_sem_init(value: usize) -> Result<sem_t, Errno> {
        let mut sem: sem_t = unsafe { std::mem::zeroed() };
        let res = unsafe {
            sem_init(&mut sem, 0 /*0 => shared between threads */, value as u32)};
        if res == 0 {
            Ok(sem)
        } else {
            Err(Errno::from_raw(Errno::last_raw()))
        }
    }
    
    /// Wrapper for libc::sem_wait()
    pub fn kv_sem_wait(sem: &mut sem_t) -> Result<(), Errno>{
        let res = unsafe {sem_wait(sem)};
        if res == 0 {
            Ok(())
        } else {
            Err(Errno::from_raw(Errno::last_raw()))
        }
    }
    
    /// Wrapper for libc::sem_post()
    pub fn kv_sem_post(sem: &mut sem_t) -> Result<(), Errno>{
        let res = unsafe {sem_post(sem)};
        if res == 0 {
            Ok(())
        } else {
            Err(Errno::from_raw(Errno::last_raw()))
        }
    }
}

pub mod syncdindex {
    use std::{collections::HashMap, path::Path};
    use nix::{errno::Errno, fcntl::{OFlag, open}, libc::pthread_mutex_t, sys::{socket::sockopt::ReuseAddr, stat::Mode}};
    use crate::{io::{KEY_SIZE, KVKey, KVLogItem, KVValue}, semaphores::{kv_mutex_init, kv_mutex_lock, kv_mutex_unlock}};

    const LOGITEM_SIZE: usize = 28;
    const ENTRY_SIZE: usize = KEY_SIZE + LOGITEM_SIZE;

    pub struct SyncdIndex {
        pub map: HashMap<KVKey, KVLogItem>,
        mtx: pthread_mutex_t,
    }

    impl SyncdIndex {
        pub fn new() -> Self {
            Self {
                map: HashMap::new(),
                mtx: kv_mutex_init().unwrap(),
            }
        }

        /* todo: init index from log file */
        pub fn from_file(path: &Path) -> Result<Self,Errno> {
            /* open persisted index file */
            let index_fd = match open(
                path, 
                OFlag::O_RDONLY,
                Mode::S_IRWXU
            ){
                Ok(fd) => fd,
                Err(Errno::ENOENT) => return Err(Errno::ENOENT),
                Err(e) => {
                    eprintln!("from_file::open() err: {}", e);
                    return Err(e);
                }
            };

            let mut buf = [0u8; 4096 * 4];
            let mut bytes_read = 0;
            loop {
                match nix::unistd::read(&index_fd, &mut buf){
                    Ok(0) => break,
                    Ok(n) => bytes_read += n,
                    Err(e) => {
                        eprintln!("from_file::read() err: {}", e);
                        return Err(e);
                    },
                }
            }

            /* invalid byte size, should be factor of kv_shared::syncdindex::ENTRY_SIZE */
            if bytes_read % ENTRY_SIZE != 0 { 
                eprintln!("from_file: expected to read (n * ENTRY_SIZE) bytes but instead got {}", bytes_read);
                return Err(Errno::EINVAL); 
            } 

            /* read entries into new hashmap */
            let mut sindex = SyncdIndex::new();
            let entries = bytes_read / ENTRY_SIZE;
            for i in 0..entries{
                let offset = i * ENTRY_SIZE;
                let key = KVKey::from_bytes(&buf[offset..offset+KEY_SIZE]).unwrap();
                let item = KVLogItem::from_bytes(&buf[offset+KEY_SIZE..offset+KEY_SIZE+LOGITEM_SIZE]).unwrap();
                match sindex.map.insert(key, item){
                    None => None,
                    Some(log_item) => {
                        eprintln!("from_file: repeated key unexpectedly found while rebuilding map");
                        Some(log_item)
                    }
                };
            }
            Ok(sindex)

        }

        /* todo: remove this */
        pub fn to_file(&self, path: &Path) -> Result<(), Errno>{
            /* open file for persist */
            let index_fd = open(
                path, 
                OFlag::O_WRONLY
                | OFlag::O_TRUNC 
                | OFlag::O_CREAT
                | OFlag::O_APPEND, 
                Mode::S_IRWXU
            ).unwrap();
            
            //println!("syncdindex contains {} entries", self.map.len());

            /* loop over map and append k:i to file */
            for (key, item) in self.map.iter(){
                let mut bytes: Vec<u8> = Vec::new();
                bytes.extend(key.to_bytes());   // 256              = 256
                bytes.extend(item.to_bytes());  // 8 + 8 + 8 + 4    = 28
                                                                    //    = 284

                let mut bytes_written = 0;
                while bytes_written < bytes.len(){
                    match nix::unistd::write(&index_fd, &bytes[bytes_written..]){
                        Ok(n) => bytes_written += n,
                        Err(e) => return Err(e),
                    }
                }
            };

            nix::unistd::close(index_fd).unwrap();
            Ok(())
            
        }

        pub fn si_insert(&mut self, key: KVKey, new_val: KVLogItem) -> Result<(),Errno>{
            kv_mutex_lock(&mut self.mtx).unwrap();
            match self.map.get(&key){
                Some(v) => {
                    /* only update k:v if newer */
                    if v.time_added < new_val.time_added {
                        self.map.insert(key, new_val);
                    }
                },
                None => {
                    self.map.insert(key, new_val);
                }
            }
            kv_mutex_unlock(&mut self.mtx).unwrap();
            Ok(())
        }

        pub fn si_get(&mut self, key: KVKey) -> Option<&KVLogItem>{
            kv_mutex_lock(&mut self.mtx).unwrap();
            let result = self.map.get(&key);
            kv_mutex_unlock(&mut self.mtx).unwrap();
            result
        }

    }

}

pub mod maxheap {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub struct HeapNode{
        pub index: usize,
        pub value:  Duration,
        pub parent: Option<usize>,
        pub left:   Option<usize>,
        pub right:  Option<usize>,
    }

    impl HeapNode{
        pub fn new(val: Duration, index: usize) -> Self {
            Self {index, value: val, parent: None, left: None, right: None }
        }
    }
 
    pub struct MaxTimeHeap {
        nodes: Vec<HeapNode>,
        root: Option<usize>,
    }

    impl MaxTimeHeap {
        pub fn new() -> Self {
            Self { nodes: Vec::new(), root: None }
        }

        /* add new node between parent and child */
        fn heap_insert(&mut self, new_val: Duration, parent: Option<HeapNode>, child: Option<HeapNode>){

            let new_node_index =  self.nodes.len();
            if parent.is_none() {
                /* new root */
                /* wip...
                self.root = Some(new_node_index);
                self.nodes.push(HeapNode::new(new_val, self.nodes.len()));
                self.nodes[new_node_index].left = Some(&child.unwrap().index);
                self.nodes[child.unwrap().index].parent = Some(new_node_index);
                 */

            } else if child.is_none() {
                /* new leaf */
            } else {
                /* inserting between parent and child */

            }


        }

        pub fn push(&mut self, val: Duration){
            /* heap is empty */
            if self.root.is_none() {
                self.root = Some(self.nodes.len());
                self.nodes.push(HeapNode::new(val, self.nodes.len()));
                return;
            } else {
                /* heap is not empty, add leaf */
                /* wip...
                let mut curr_nodes: Vec<&HeapNode> = vec![&self.nodes[self.root.unwrap()]];
                loop {
                    for node in curr_nodes{
                        if val > node.value {
                            //heap_insert(self, val, None, Some(node));
                        }
                    }
                }
                 */
                    
            }
        }


    }
}