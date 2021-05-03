#![allow(unused)]

use bimap::BiMap;
use k8s_openapi::api::core::v1::Namespace;
use polyfuse::{op, reply::*, KernelConfig, Operation, Request, Session};
use std::collections::HashMap;
use std::convert::TryFrom as _;
use std::ffi::{OsStr, OsString};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

mod async_session;

const INO_ROOT: u64 = 1;
const INO_DIR_FREESTANDING: u64 = 2;
const INO_ALLOC_START: u64 = 3;

const TTL_FOREVER: Duration = Duration::from_secs(60 * 60 * 24 * 365);
const TTL_SHORT: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    pretty_env_logger::try_init_custom_env("F8S_LOG")?;

    let mountpoint = Path::new("./mnt");
    eyre::ensure!(mountpoint.is_dir(), "./mnt must be a dir");

    let mut config = KernelConfig::default();

    // polyfuse hardcodes `/usr/bin/fusermount`
    #[cfg(feature = "polyfuse-undocumented")]
    config.fusermount_path("/run/wrappers/bin/fusermount");

    let session = Session::mount(mountpoint.into(), config)?;

    let client = kube::Client::try_default().await?;

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut map = BiMap::<u64, Entry>::new();
    let mut next_ino = AtomicU64::new(INO_ALLOC_START);

    let mut next_fh = AtomicU64::new(0x10000);

    let mut opened_dirs = HashMap::<u64, Vec<(OsString, u64, u32)>>::new();

    while let Some(req) = session.next_request()? {
        log::debug!("request {:#x}:", req.unique());
        // log::debug!("  - uid: {:?}", req.uid());
        // log::debug!("  - gid: {:?}", req.gid());
        // log::debug!("  - pid: {:?}", req.pid());

        let op = req.operation()?;
        log::debug!("  - op: {:?}", op);

        match op {
            Operation::Getattr(op) if op.ino() == INO_ROOT => {
                log::info!("getattr on mountpoint");

                let mut out = AttrOut::default();
                out.ttl(TTL_FOREVER);

                let attrs = out.attr();
                attrs.ino(op.ino());
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(2);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            Operation::Getattr(op) if op.ino() == INO_DIR_FREESTANDING => {
                log::info!("getattr on /_/");

                let mut out = AttrOut::default();
                out.ttl(TTL_FOREVER);

                let attrs = out.attr();
                attrs.ino(op.ino());
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(2);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            Operation::Getattr(op) if map.contains_left(&op.ino()) => {
                let entry = map.get_by_left(&op.ino()).unwrap();

                log::info!("getattr on entry: {:?}", entry);

                let mut out = AttrOut::default();
                out.ttl(TTL_SHORT);

                let attrs = out.attr();
                attrs.ino(op.ino());
                attrs.uid(uid);
                attrs.gid(gid);

                match entry {
                    Entry::Namespace { .. } => {
                        attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                        attrs.nlink(2);
                    }
                    Entry::ObjectNamespaced { .. } => {
                        attrs.mode(libc::S_IFREG | libc::S_IRUSR | libc::S_IXUSR);
                    }
                    Entry::ObjectFreestanding { .. } => {
                        attrs.mode(libc::S_IFREG | libc::S_IRUSR | libc::S_IXUSR);
                    }
                }

                req.reply(out)?;
            }

            Operation::Getattr(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());

                log::warn!("getattr on something not found");
                req.reply_error(libc::ENOENT)?;
            }

            Operation::Lookup(op) if op.parent() == INO_ROOT && op.name() == "_" => {
                log::info!("lookup on /_/");

                let mut out = EntryOut::default();
                out.ino(INO_DIR_FREESTANDING);
                out.ttl_attr(TTL_FOREVER);
                out.ttl_entry(TTL_FOREVER);

                let attrs = out.attr();
                attrs.ino(INO_DIR_FREESTANDING);
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(2);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            Operation::Lookup(op)
                if op.parent() == INO_ROOT
                    && map.contains_right(&Entry::Namespace {
                        name: op.name().to_str().unwrap().into(),
                    }) =>
            {
                let ino = map
                    .get_by_right(&Entry::Namespace {
                        name: op.name().to_str().unwrap().into(),
                    })
                    .copied()
                    .unwrap();

                log::info!(
                    "lookup on namespace entry: name: {:?}, ino: {:?}",
                    op.name(),
                    ino
                );

                let mut out = EntryOut::default();
                out.ino(ino);
                out.ttl_attr(TTL_SHORT);
                out.ttl_entry(TTL_SHORT);

                let attrs = out.attr();
                attrs.ino(ino);
                attrs.mode(libc::S_IFDIR | libc::S_IRUSR | libc::S_IXUSR);
                attrs.nlink(2);
                attrs.uid(uid);
                attrs.gid(gid);

                req.reply(out)?;
            }

            // TODOO lookup {namespace} when not in map
            // TODO: lookup _/{name}.{kind}.yaml
            // TODO: lookup {namespace}/{name}.{kind}.yaml
            Operation::Lookup(op) => {
                log::debug!("    - parent: {:?}", op.parent());
                log::debug!("    - name: {:?}", op.name());

                log::warn!("lookup with no matching case");
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Opendir(op) if op.ino() == INO_ROOT => {
                log::info!("opendir on mountpoint");

                let fh = next_fh.fetch_add(1, Ordering::SeqCst);

                let mut entries = vec![
                    (".".into(), INO_ROOT, libc::DT_DIR as u32),
                    ("..".into(), INO_ROOT, libc::DT_DIR as u32),
                    ("_".into(), INO_DIR_FREESTANDING, libc::DT_DIR as u32),
                ];

                let namespaces = kube::Api::<Namespace>::all(client.clone());

                for ns in namespaces.list(&Default::default()).await?.items {
                    let entry = Entry::from_namespace(&ns);
                    let ino = map.get_by_right(&entry).copied().unwrap_or_else(|| {
                        let ino = next_ino.fetch_add(1, Ordering::SeqCst);
                        let overwritten = map.insert(ino, entry);

                        if overwritten.did_overwrite() {
                            log::error!(
                                "wrote over something in the ino/entry mapping: {:?}",
                                overwritten
                            );
                        }

                        ino
                    });

                    let name = ns.metadata.name.unwrap();
                    entries.push((OsString::from(name), ino, libc::DT_DIR as u32));
                }

                opened_dirs.insert(fh, entries);

                let mut out = OpenOut::default();
                out.fh(fh);
                out.direct_io(true); // TODO: no clue what this does

                req.reply(out)?;
            }

            // TODO: _/
            // TODO: {namespace}/
            // TODO: reply_error when ISREG
            Operation::Opendir(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Readdir(op) if op.mode() == op::ReaddirMode::Plus => {
                log::warn!("readdir+");

                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Readdir(op) if opened_dirs.contains_key(&op.fh()) => {
                log::info!("readdir on opened dir: fh: {:?}", op.fh());
                log::debug!("offset: {:?}", op.offset());

                let entries = opened_dirs.get(&op.fh()).unwrap();

                let mut out = ReaddirOut::new(op.size() as usize);

                for (offset, entry) in entries.iter().enumerate().skip(op.offset() as usize) {
                    let filled = out.entry(&entry.0, entry.1, entry.2, offset as u64 + 1);

                    if filled {
                        break;
                    };
                }

                req.reply(out)?;
            }

            Operation::Readdir(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());
                log::debug!("    - offset: {:?}", op.offset());
                log::debug!("    - size: {:?}", op.size());
                log::debug!("    - mode: {:?}", op.mode());

                log::warn!("readdir with no matching case");
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Releasedir(op) if opened_dirs.contains_key(&op.fh()) => {
                log::info!("releasing dir: fh: {:?}", op.fh());
                opened_dirs.remove(&op.fh());
            }

            Operation::Releasedir(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            // TODO: _/{name}.{kind}.yaml
            // TODO: {namespace}/{name}.{kind}.yaml
            // TODO: reply_error when ISDIR
            Operation::Open(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Read(op) => {
                log::info!("    - ino: {:?}", op.ino());
                log::info!("    - fh: {:?}", op.fh());
                log::info!("    - offset: {:?}", op.size());
                log::info!("    - size: {:?}", op.size());
                log::info!("    - flags: {:?}", op.flags());
                log::info!("    - lock_owner: {:?}", op.lock_owner());

                log::warn!("read with no matching case");
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Release(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            _ => req.reply_error(libc::ENOSYS)?,
        }
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum Entry {
    // {name}/
    Namespace {
        name: String,
    },
    // {namespace}/{name}.{kind}.yaml
    ObjectNamespaced {
        namespace: String,
        name: String,
        kind: String,
    },
    // _/{name}.{kind}.yaml
    ObjectFreestanding {
        name: String,
        kind: String,
    },
}

impl Entry {
    fn is_dir(&self) -> bool {
        matches!(self, Entry::Namespace { .. })
    }

    fn is_reg(&self) -> bool {
        !self.is_dir()
    }

    fn from_namespace(ns: &Namespace) -> Self {
        Self::Namespace {
            name: ns.metadata.name.as_ref().cloned().unwrap(),
        }
    }

    // fn object_from_resource(resource: &impl kube::Resource) -> Self {
    //     match resource.namespace() {
    //         Some(namespace) => {
    //             todo!()
    //         }
    //         None => Self::ObjectFreestanding {
    //             name: resource.name(),
    //             kind: resource.kind().into(),
    //         },
    //     }
    // }

    // fn from_resource(resource: &impl kube::Resource) -> Self {
    //     todo!()
    // }
}
