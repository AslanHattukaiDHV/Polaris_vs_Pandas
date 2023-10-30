from ast import main
from os import name
from threading import main_thread
import click
from memory_profiler import memory_usage
import matplotlib.pyplot as plt

import write_benchmark


@click.group()
@click.option(
    "--pvp",
    type=click.Choice(["pd", "pl", "both"]),
    default="both",
    required=True,
    help="Select the library to benchnmark (valid choices: pd (pandas), pl (polars or both (default)).",
)
@click.pass_context
def bears(ctx, pvp: str):
    ctx.ensure_object(dict)

    ctx.obj.update({
        'pvp': pvp
    })


# run writing benchmarks
@bears.command()
@click.option("--num_rows", default=1000000, help="Choose number of rows.")
@click.option("--num_int_cols", default=5, help="Choose number of integer columns.")
@click.option("--num_float_cols", default=5, help="Choose number of float columns.")
@click.pass_context
def writing(ctx, num_rows: int, num_int_cols: int, num_float_cols: int):
    ctx.obj.update({
        "num_rows": num_rows,
        "num_int_cols": num_int_cols,
        "num_float_cols": num_float_cols
    })

    results = memory_usage(
        proc=(write_benchmark.test_write, (), ctx.obj),
        interval=.1
    )

    plt.plot([s*.1 for s in range(len(results))], results)
    plt.ylabel('Writing benchmark')
    plt.savefig('foo.png')


if __name__ == "__main__":
    bears(auto_envvar_prefix='BEAR')