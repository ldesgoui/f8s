//  mountpoint
//      .not-namespaced
//          default.ns.yaml
//          kube-system.ns.yaml
//      default
//      kube-system
//          coredns.deploy.yaml
//          coredns-74ff55c5b.rs.yaml
//          coredns-74ff55c5b-xjt82.pod.yaml

#![allow(unused)]

use bimap::BiMap;
use k8s_openapi::api::core::v1::Namespace;
use polyfuse::{op, reply::*, KernelConfig, Operation, Request, Session};
use std::convert::TryFrom as _;
use std::ffi::OsString;
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

    let mut config = kube::Config::infer().await?;
    config.accept_invalid_certs = true;
    let client = kube::Client::try_from(config)?;

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut map = BiMap::<u64, Entry>::new();
    let mut next_ino = AtomicU64::new(INO_ALLOC_START);

    while let Some(req) = session.next_request()? {
        log::debug!("request {:#x}:", req.unique());
        log::debug!("  - uid: {:?}", req.uid());
        log::debug!("  - gid: {:?}", req.gid());
        log::debug!("  - pid: {:?}", req.pid());

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

            Operation::Lookup(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Opendir(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Readdir(op) if op.mode() == op::ReaddirMode::Plus => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Readdir(op) if op.ino() == INO_ROOT => {
                log::info!("readdir on mountpoint");

                if op.offset() != 0 {
                    req.reply(ReaddirOut::new(op.size() as usize))?;
                    continue;
                }

                let mut out = ReaddirOut::new(op.size() as usize);

                out.entry(&OsString::from("."), INO_ROOT, libc::DT_DIR as u32, 1);
                out.entry(&OsString::from(".."), INO_ROOT, libc::DT_DIR as u32, 2);
                out.entry(
                    &OsString::from("_"),
                    INO_DIR_FREESTANDING,
                    libc::DT_DIR as u32,
                    3,
                );

                let namespaces = kube::Api::<Namespace>::all(client.clone());

                for ns in namespaces.list(&Default::default()).await?.items {
                    let name = ns.metadata.name.unwrap();
                    let ino = map
                        .get_by_right(&Entry::Namespace { name: name.clone() })
                        .copied()
                        .unwrap_or_else(|| 1 + 1);

                    out.entry(&OsString::from(name), ino, libc::DT_DIR as u32, 69);
                }

                req.reply(out)?;
            }

            Operation::Readdir(op) => {
                log::debug!("    - ino: {:?}", op.ino());
                log::debug!("    - fh: {:?}", op.fh());
                log::debug!("    - offset: {:?}", op.offset());
                log::debug!("    - size: {:?}", op.size());
                log::debug!("    - mode: {:?}", op.mode());

                log::warn!("readdir on something not found");
                req.reply_error(libc::ENOENT)?;
            }

            Operation::Releasedir(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Open(op) => {
                req.reply_error(libc::ENOSYS)?;
            }

            Operation::Read(op) if op.ino() == INO_ROOT => {
                log::warn!("read on mountpoint");
                req.reply_error(libc::EISDIR)?;
            }

            Operation::Read(op) => {
                log::info!("    - ino: {:?}", op.ino());
                log::info!("    - fh: {:?}", op.fh());
                log::info!("    - offset: {:?}", op.size());
                log::info!("    - size: {:?}", op.size());
                log::info!("    - flags: {:?}", op.flags());
                log::info!("    - lock_owner: {:?}", op.lock_owner());

                log::warn!("read on something not found");
                req.reply_error(libc::ENOENT)?;
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
