#!/usr/bin/env python
'''
A script to help/test with parcp (a library for running CellProfiler in
                                  parallel).

@author Yauhen Yakimovich <eugeny.yakimovitch@gmail.com>
'''
import os
# TODO: add arg parsing
# import sys
import logging
from parcp import ParallelCellProfiler

logger = logging.getLogger('parcp2')

PROJECT = 'ExampleFlyImages'


if __name__ == '__main__':
    # TODO: Test on sample project
    logging.basicConfig(level=logging.INFO)

    project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                PROJECT)
    runner = ParallelCellProfiler(project_path)
    runner.load_image_setting('image_groups.json')
    runner.split_images()
    runner.run_batches('ExampleFly.cppipe')
    runner.merge_results()
