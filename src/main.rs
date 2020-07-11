mod flex_buffer;

use std::convert::TryInto;
use std::process;
use std::sync::Arc;
use std::vec::Vec;

use clap::{App, AppSettings, Arg, ArgMatches};
use maligned::{aligned, A4k};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use flex_buffer::FlexBuffer;

const TCP_CONNECT: &str = "TCP_CONNECT";
const TCP_LISTEN: &str = "TCP_LISTEN";
const UDP_BIND: &str = "UDP_BIND";
const UDP_CONNECT: &str = "UDP_CONNECT";
const UDP_LISTEN: &str = "UDP_LISTEN";

type TokioVoidResult = Result<(), tokio::io::Error>;

fn tokio_ok() -> TokioVoidResult {
    Ok(())
}

async fn read_into<Reader>(reader: &mut Reader, buf: &mut FlexBuffer<'_>) -> TokioVoidResult
where
    Reader: AsyncReadExt + Unpin,
{
    let size = reader.read(buf.bytes_mut()).await?;
    buf.advance_mut(size);
    tokio_ok()
}

async fn recv_main(matches: &ArgMatches<'_>) -> TokioVoidResult {
    let mut tcp_stream = TcpStream::connect(matches.value_of(TCP_CONNECT).unwrap()).await?;
    let mut udp_sender = UdpSocket::bind(matches.value_of(UDP_BIND).unwrap()).await?;

    udp_sender
        .connect(matches.value_of(UDP_CONNECT).unwrap())
        .await?;

    let mut backing = *aligned::<A4k, _>([0u8; 128 * 1024]);
    let mut buf = FlexBuffer::new(&mut backing);

    loop {
        while buf.remaining() < 2 {
            buf.compact_if_less(2);
            read_into(&mut tcp_stream, &mut buf).await?;
        }
        let header_bytes = buf.bytes();
        let size = (usize::from(header_bytes[1]) << 8) | usize::from(header_bytes[0]);
        buf.advance(2);
        while buf.remaining() < size {
            buf.compact_if_less(size);
            read_into(&mut tcp_stream, &mut buf).await?;
        }
        let sent_size = udp_sender.send(&buf.bytes()[..size]).await?;
        assert_eq!(sent_size, size, "udp short send");
        buf.advance(size);
    }
}

async fn send_main(matches: &ArgMatches<'_>) -> TokioVoidResult {
    let tcp_listener = TcpListener::bind(matches.value_of(TCP_LISTEN).unwrap()).await?;
    let udp_listener = UdpSocket::bind(matches.value_of(UDP_LISTEN).unwrap()).await?;
    if let Some(udp_connect) = matches.value_of(UDP_CONNECT) {
        udp_listener.connect(udp_connect).await?;
    }

    let tcp_streams = Arc::new(Mutex::new(Vec::<TcpStream>::new()));

    // https://rust-lang.github.io/async-book/07_workarounds/03_err_in_async_blocks.html

    let tcp_streams_f = tcp_streams.clone();
    #[allow(unreachable_code)]
    let forwarder = tokio::spawn(async move {
        let mut udp_listener = udp_listener;
        let mut buf = aligned::<A4k, _>([0u8; 64 * 1024 + 2]);
        loop {
            let size = udp_listener.recv(&mut buf[2..]).await?;
            let write_size = 2 + size;
            // 2 byte header for LE order size
            buf[0] = (size & 0xff).try_into().unwrap();
            buf[1] = (size >> 8).try_into().unwrap();
            let mut tcp_streams_locked = tcp_streams_f.lock().await;
            let mut valid_streams = tcp_streams_locked.len();
            let mut i = 0;
            while i < valid_streams {
                if let Ok(written_size) = tcp_streams_locked[i].write(&buf[..write_size]).await {
                    if written_size == write_size {
                        // a-ok, go to next stream
                        i += 1;
                        continue;
                    }
                }
                // os buffer full, mark the stream for deletion by moving to end
                tcp_streams_locked.swap(i, valid_streams - 1);
                valid_streams -= 1;
                // process the current slot again, since the potentially valid one from the end
                // was swapped in
            }
            // actually delete the streams
            tcp_streams_locked.truncate(valid_streams);
        }
        tokio_ok()
    });

    let tcp_streams_a = tcp_streams.clone();
    #[allow(unreachable_code)]
    let accepter = tokio::spawn(async move {
        let mut tcp_listener = tcp_listener;
        loop {
            let (stream, _) = tcp_listener.accept().await?;
            let _ = stream.set_send_buffer_size(4 * 1024 * 1024);
            let mut tcp_streams_locked = tcp_streams_a.lock().await;
            tcp_streams_locked.push(stream);
        }
        tokio_ok()
    });

    if let Err(err) = tokio::try_join!(forwarder, accepter) {
        Err(tokio::io::Error::from(err))
    } else {
        Ok(())
    }
}

fn main() -> () {
    let matches = App::new("udp_in_tcp")
        .subcommand(
            App::new("recv")
                .arg(
                    Arg::with_name(TCP_CONNECT)
                        .takes_value(true)
                        .short("t")
                        .long("tcp-connect")
                        .help("address:port")
                        .required(true),
                )
                .arg(
                    Arg::with_name(UDP_BIND)
                        .takes_value(true)
                        .short("u")
                        .long("udp-bind")
                        .help("address:port")
                        .required(false)
                        .default_value("[::]:0"),
                )
                .arg(
                    Arg::with_name(UDP_CONNECT)
                        .takes_value(true)
                        .short("c")
                        .long("udp-connect")
                        .help("send datagrams to this address:port")
                        .required(true),
                ),
        )
        .subcommand(
            App::new("send")
                .arg(
                    Arg::with_name(TCP_LISTEN)
                        .takes_value(true)
                        .short("t")
                        .long("tcp-listen")
                        .help("address:port")
                        .required(true),
                )
                .arg(
                    Arg::with_name(UDP_CONNECT)
                        .takes_value(true)
                        .short("c")
                        .long("udp-connect")
                        .help("only recv datagrams from this address:port")
                        .required(false),
                )
                .arg(
                    Arg::with_name(UDP_LISTEN)
                        .takes_value(true)
                        .short("u")
                        .long("udp-listen")
                        .help("address:port")
                        .required(true),
                ),
        )
        .setting(AppSettings::SubcommandRequired)
        .get_matches();

    let mut rt = Runtime::new().unwrap();

    let result = if let Some(ref matches) = matches.subcommand_matches("recv") {
        rt.block_on(recv_main(&matches))
    } else if let Some(ref matches) = matches.subcommand_matches("send") {
        rt.block_on(send_main(&matches))
    } else {
        panic!("subcommand not implemented");
    };

    if let Err(err) = result {
        eprintln!("{}", err);
        process::exit(1);
    }

    ()
}
