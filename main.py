#!/usr/bin/env python

import os
import sys
import argparse

import crawl
import extract
import transform


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--crawl')
    args = parser.parse_args()

    if args.crawl == 'yes':
        c = raw_input("This will crawl the websites and will take very long time. Do you want to continue? ")

        if c == 'yes':
            crawl.crawl_trains()

    extract.main()
    transform.main()
    os.system('bash export.sh')


if __name__ == '__main__':
    main()
