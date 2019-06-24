#!/usr/bin/env python3
import argparse
def main():
    parser = argparse.ArgumentParser(description='Compare two outputs')
    parser.add_argument('file1', type=str, help='Expected output')
    parser.add_argument('file2', type=str, help='Real output')
    args = parser.parse_args()
    with open(args.file2) as real:
        real_lines = real.readlines()
        with open(args.file1) as expected:
            exp_lines=0
            for line in expected:
                exp_lines = exp_lines + 1
                found = False
                for real_line in real_lines:
                    if real_line == line:
                        found = True
                        break
                if not found:
                    print( "Could not match line:" )
                    print( line )
                    return

    if not ( exp_lines == len(real_lines) ):
        print( "Too many lines in real output." )
        return
    print( "Output matched." )
if __name__== "__main__":
  main()
