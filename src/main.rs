mod flex_buffer;

use std::convert::TryInto;
use std::process;
use std::sync::Arc;
use std::vec::Vec;

use clap::{App, AppSettings, Arg, ArgMatches};
use maligned::{A4k, aligned, align_first};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::runtime::Runtime;
use tokio::sync::broadcast;

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

async fn read_into<Reader>(reader: &mut Reader, buf: &mut FlexBuffer<'_>) -> Result<usize, tokio::io::Error>
where
    Reader: AsyncReadExt + Unpin,
{
    let size = reader.read(buf.bytes_mut()).await?;
    buf.advance_mut(size);
    Ok(size)
}

async fn recv_main(matches: &ArgMatches<'_>) -> TokioVoidResult {
    let mut tcp_stream = TcpStream::connect(matches.value_of(TCP_CONNECT).unwrap()).await?;
    let mut udp_sender = UdpSocket::bind(matches.value_of(UDP_BIND).unwrap()).await?;

    udp_sender
        .connect(matches.value_of(UDP_CONNECT).unwrap())
        .await?;

    let mut backing = *aligned::<A4k, _>([0u8; 128 * 1024]);
    let mut buf = FlexBuffer::new(&mut backing);

    'outer: loop {
        while buf.remaining() < 2 {
            buf.compact_if_less(2);
            let read_size = read_into(&mut tcp_stream, &mut buf).await?;
            if read_size == 0 {
                break 'outer;
            }
        }
        let header_bytes = buf.bytes();
        let size = (usize::from(header_bytes[1]) << 8) | usize::from(header_bytes[0]);
        buf.advance(2);
        while buf.remaining() < size {
            buf.compact_if_less(size);
            let read_size = read_into(&mut tcp_stream, &mut buf).await?;
            if read_size == 0 {
                break 'outer;
            }
        }
        let _ = udp_sender.send(&buf.bytes()[..size]).await;
        buf.advance(size);
    }

    tokio_ok()
}

async fn tcp_sender(mut receiver: broadcast::Receiver<Arc<Vec<u8>>>, mut tcp_stream: TcpStream) -> TokioVoidResult {
    loop {
        match receiver.recv().await {
            Err(_) => {
                return tokio_ok();
            },
            Ok(data_arc) => {
                tcp_stream.write_all(&data_arc[..]).await?;
            },
        };
    }
}

async fn send_main(matches: &ArgMatches<'_>) -> TokioVoidResult {
    let tcp_listener = TcpListener::bind(matches.value_of(TCP_LISTEN).unwrap()).await?;
    let udp_listener = UdpSocket::bind(matches.value_of(UDP_LISTEN).unwrap()).await?;
    if let Some(udp_connect) = matches.value_of(UDP_CONNECT) {
        udp_listener.connect(udp_connect).await?;
    }

    let (send_channel, _) = broadcast::channel::<Arc<Vec<u8>>>(1024);

    // https://rust-lang.github.io/async-book/07_workarounds/03_err_in_async_blocks.html

    let send_channel_f = send_channel.clone();
    #[allow(unreachable_code)]
    let forwarder = tokio::spawn(async move {
        let mut udp_listener = udp_listener;
        loop {
            let mut buf = align_first::<u8, A4k>(2 + 64 * 1024);
            buf.resize(buf.capacity(), 0u8);
            let size = udp_listener.recv(&mut buf[2..]).await?;
            // 2 byte header for LE order size
            buf[0] = (size & 0xff).try_into().unwrap();
            buf[1] = (size >> 8).try_into().unwrap();
            buf.truncate(size + 2);
            let _ = send_channel_f.send(Arc::new(buf));
        }
        tokio_ok()
    });

    #[allow(unreachable_code)]
    let accepter = tokio::spawn(async move {
        let mut tcp_listener = tcp_listener;
        loop {
            let (stream, _) = tcp_listener.accept().await?;
            tokio::spawn(tcp_sender(send_channel.subscribe(), stream));
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
