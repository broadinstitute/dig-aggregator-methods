import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default=None, required=True, type=str,
                        help="Dataset name")
    parser.add_argument('--cell-type', default=None, required=True, type=str,
                        help="Cell Type")
    parser.add_argument('--model', default=None, required=True, type=str,
                        help="Model")
    args = parser.parse_args()

    import time
    time.sleep(3600)


if __name__ == '__main__':
    main()
