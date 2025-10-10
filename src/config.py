from windprofiles import Parser
import pathlib
import os

parent_dir = pathlib.Path(__file__).parent.parent
results_dir = os.path.join(parent_dir, "results")
for r in ["analysis", "figures", "processed", "testing"]:
    s = os.path.join(results_dir, r)
    os.makedirs(s, exist_ok = True)

parser = Parser(paths=["data"], define=["process"])


def parse(mode: str):
    match mode:
        case "process":
            parser.add_argument("--nproc", "-n", type=int, metavar="N", help="Number of processors to use in multiprocessing (note - one will be used for log listening)")
            parser.add_argument("--only", "-o", type=str, metavar="NAME", help="Single directory to process, by key name in [process] config section")
            parser.add_argument("--test", "-t", action="store_true", help="Short run for testing purposes")
            return parser.parse()
        case "interact":
            parser.add_argument("selection", type=str, help="Key of directory to inspect interactively")
            parser.add_argument("--test", "-t", action="store_true", help="Use testing output data")
            return parser.parse()
