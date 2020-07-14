import click
import logging

from adit import starter, installer
from adit import constants as const
from adit import shutdown_handler


@click.group()
def cli() -> None:
    pass


@cli.command(help="Setup working directory for Adit")
@click.argument('workdir', default=const.DEFAULT_WORK_DIR)
def install(workdir: str = const.DEFAULT_WORK_DIR) -> None:
    installer.install(workdir=workdir)


@cli.command(help="Start Adit server")
def server() -> None:
    starter.start(mode=const.SERVER_MODE, args=None)


@cli.command(help="Start Adit client")
@click.option('-s', '--server-ip', help='TEXT = IP address of server node.', required=True)
def client(server_ip: str = None) -> None:
    starter.start(mode=const.CLIENT_MODE, args=dict({'server_ip': server_ip}))


if __name__ == "__main__":
    logging.basicConfig(
        format='[%(asctime)s][%(levelname)8s] %(filename)s:%(lineno)s | %(name)s.%(funcName)s() - %(message)s',
        level=logging.DEBUG)
    shutdown_handler.init()
    cli()
