"""Adit

Usage:
  main.py server [--config=<config-file>] [--workdir=<work-dir>]
  main.py client [--config=<config-file>] [--workdir=<work-dir>] (--master=<master-ip>)
  main.py (-h | --help)
  main.py --version

Options:
  --master=<master-ip>     IP address of master node
  --workdir=<work-dir>     Working directory where data and log files are stored. If not specified, it will use the current directory.
  --config=<config-file>   Path to the config file for Adit.
  -h --help                Show this screen.
  --version                Show version.
"""
import click
from adit import boot
from adit import constants as const


@click.group()
def cli():
    pass


@cli.command()
@click.option('-c', '--config', help='TEXT = Path to the config file for Adit', required=False)
@click.option('-d', '--workdir', help='TEXT = Working directory where data and log files are stored. '
                                      'If not specified, it will use the current directory.', required=False)
def server(config, workdir):
    boot.boot(mode=const.SERVER_MODE, args=dict({'config': config, 'workdir': workdir}))


@cli.command()
@click.option('-s', '--server-ip', help='TEXT = IP address of server node.', required=True)
@click.option('-c', '--config', help='TEXT = Path to the config file for Adit', required=False)
@click.option('-d', '--workdir', help='TEXT = Working directory where data and log files are stored. '
                                      'If not specified, it will use the current directory.', required=False)
def client(server_ip, config, workdir):
    boot.boot(mode=const.CLIENT_MODE, args=dict({'server-ip': server_ip, 'config': config, 'workdir': workdir}))


if __name__ == "__main__":
    cli()
