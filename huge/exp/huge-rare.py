import argparse
import math
from scipy.stats import norm


def p_to_z(p_value):
    return abs(norm.ppf(p_value / 2.0))


def calculate_bf_rare(p, beta):
    z = p_to_z(p)
    bf_rare_min = 1
    bf_rare_max = 348
    if p == 0 or z == math.inf or z == -math.inf:
        return bf_rare_max
    else:
        std_err = beta / z
        v = std_err * std_err
        omega = 0.3696
        bf_rare = math.sqrt(v / (v + omega)) * math.exp(omega * beta * beta / (2.0 * v * (v + omega)))
        if bf_rare < bf_rare_min:
            return bf_rare_min
        elif bf_rare > bf_rare_max:
            return bf_rare_max
        else:
            return bf_rare


def main():
    """
    Arguments: none
    """
    arg_parser = argparse.ArgumentParser(prog='huge-rare.py')
    arg_parser.add_argument("--p", help="p-value", required=True)
    arg_parser.add_argument("--beta", help="beta", required=True)
    cli_args = arg_parser.parse_args()
    p = float(cli_args.p)
    beta = float(cli_args.beta)
    bf_rare = calculate_bf_rare(p, beta)
    print(bf_rare)


if __name__ == '__main__':
    main()
