import csv
import argparse

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

URL = 'ws://localhost:8182/gremlin'

def get_lines(g, is_edge, label):
    """
    Returns all rows of edge or vertex of the specified label.
    """

    if is_edge:
        return g.E().hasLabel(label).toList()

    return g.V().hasLabel(label).toList()

def get_key_list(g, is_edge, line):
    """
    Returns all header information.
    """

    if is_edge:
        return g.E(line).properties().key().toList()

    return g.V(line).properties().key().toList()

def get_line(g, is_edge, line):
    """
    Returns information on the specified line.
    """

    if is_edge:
        return g.E(line).values().toList()

    return g.V(line).values().toList()

def main():
    """
    Export graph data to CSV format.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("label", type=str, help="Please specify TYPE or LABEL")
    parser.add_argument("-e", "--edge", action="store_true", help="output edge")

    args = parser.parse_args()

    graph = Graph()
    g = graph.traversal().withRemote(DriverRemoteConnection(URL, 'g'))

    with open(args.label + ".csv", "w", newline="") as outf:
        writer = csv.writer(outf, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        lines = get_lines(g, args.edge, args.label)

        key_list = get_key_list(g, args.edge, lines[0])
        key_list.append('label')
        writer.writerow(key_list)

        for line in lines:
            tmp = get_line(g, args.edge, line)
            tmp.append(args.label)
            writer.writerow(tmp)

if __name__ == "__main__":
    main()
