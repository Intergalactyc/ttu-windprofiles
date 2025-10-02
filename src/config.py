from windprofiles import Parser
import pathlib
import os

parent_dir = pathlib.Path(__file__).parent.parent
results_dir = os.path.join(parent_dir, "results")
for r in ["analysis", "figures", "processed", "testing"]:
    s = os.path.join(results_dir, r)
    os.makedirs(s, exist_ok = True)

parser = Parser(paths=["data"], define=["process"])
parser.add_argument("--nproc", "-n", type=int, metavar="N", help="Number of processors to use in multiprocessing")
parser.add_argument("--only", "-o", type=str, metavar="NAME", help="Single directory to process, by key name in [process] config section")
parser.add_argument("--test", "-t", action="store_true", help="Short run for testing purposes")

def parse():
    return parser.parse()
