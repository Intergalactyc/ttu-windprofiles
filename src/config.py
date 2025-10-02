from windprofiles import Parser

parser = Parser(paths=["data"], define=["process"])
parser.add_argument("--nproc", "-n", type=int, metavar="N", help="Number of processors to use in multiprocessing")
parser.add_argument("--only", "-o", type=str, metavar="NAME", help="Single directory to process, by key name in [process] config section")

def parse():
    return parser.parse()
