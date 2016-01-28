#!/usr/bin/env python
'''
A simple script to automate running of CellProfiler2 in parallel mode.

@author Yauhen Yakimovich <eugeny.yakimovitch@gmail.com>
'''
import os
import logging
import textwrap
from glob import glob

from cpimages import CellProfilerImages, NoImageFilesFound


logger = logging.getLogger('runcp2')


PROJECT = 'ExampleFlyImages'


def get_cp2_call():
    '''
    Requires a symlink pointing from a home forlder to the actual
    location of CP2. E.g.:

        cd && ln -s ~/dev/pelkmanslab/CellProfiler-2.1.1 CellProfiler2

    '''
    return 'python ~/CellProfiler2/CellProfiler.py'


class Project(object):

    def __init__(self, project_path):
        self.path = project_path

    @property
    def images_path(self):
        return os.path.join(self.path, 'images')

    @property
    def image_groups_path(self):
        return os.path.join(self.path, 'image_groups')

    @property
    def results_path(self):
        return os.path.join(self.path, 'results')


class RunCP2(object):

    def __init__(self, project_path):
        self.project = Project(project_path)

    def split_images(self, image_list_settings_filename):
        '''
        Make CSV lists of images for each CP2 batch. Such CSV files are an
        "input argument", telling which images to process.
        '''
        logger.info('Splitting images in: %s', self.project.path)

        images_path = self.project.images_path
        output_path = self.project.image_groups_path
        if not os.path.exists(output_path):
            logger.info('Create missing output path: %s',  output_path)
            os.makedirs(output_path)

        if not image_list_settings_filename.startswith('/') \
                and not os.path.exists(image_list_settings_filename):
            logger.info('Setting file to group images not found: %s',
                        image_list_settings_filename)
            image_list_settings_filename = os.path.join(
                self.project.path,
                image_list_settings_filename)
            if not os.path.exists(image_list_settings_filename):
                # Still missing
                raise IOError('Setting file to group images not found: %s',
                              image_list_settings_filename)
        cpimages = CellProfilerImages()
        if os.path.exists(image_list_settings_filename):
            # Load custom JSON settings file
            logger.info('Parsing grouping settings fro images: %s',
                        image_list_settings_filename)
            cpimages.parse_settings(image_list_settings_filename)

        logger.info('Splitting images into filenames')
        cpimages.split_images(images_path, output_path)
        num_of_image_sets = cpimages.set_num

        if num_of_image_sets == 0:
            print 'Error, splitting of images failed. No image set were '\
                  'generated.'
            exit(1)

        logger.info('Done. Split input images into %d lists' %
                    num_of_image_sets)
        return cpimages

    def run_cp2_pipeline_batch(self, cp_pipeline_file, input_csv_filepath):
        '''
        Execute CellProfiller2 process in a command line. Pass arguments
        sufficient to process a single batch of images.
        '''
        images_path = self.project.images_path
        results_path = self.project.results_path
        command_lines = textwrap.wrap('''
        %(cp2_call)s -b -c -i %(images_path)s -o %(results_path)s \
            --do-not-build --do-not-fetch --pipeline=%(cp_pipeline_file)s \
            --data-file=%(csv_filepath)s -L INFO
        ''' % {
            'cp2_call': get_cp2_call(),
            'images_path': images_path,
            'results_path': results_path,
            'cp_pipeline_file': cp_pipeline_file,
            'csv_filepath': input_csv_filepath,
        }, width=210, break_on_hyphens=False, break_long_words=False)
        command = ' \\\n'.join(command_lines)
        print command

    def run_batches(self, pipeline_filename):
        '''
        For each input CSV file found run a CP2 job and produce output.
        '''
        pipeline_filepath = os.path.join(self.project.path, pipeline_filename)
        image_groups = glob(os.path.join(self.project.image_groups_path,
                            'image_set_*.csv'))
        image_groups = sorted(image_groups)
        for image_group in image_groups:
            logger.info('Running cp2 with image group: %s', image_group)
            self.run_cp2_pipeline_batch(pipeline_filepath, image_group)
            # TODO: append image set to the output path as additional subfolder
            # for merging

    def merge_results(self):
        '''
        Each job produces output stored as CSV (ExportToSpreadSheet module).
        I.e. we can run only those CP2 pipelines that contain export to CSV
        module.
        '''


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    project_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                PROJECT)
    runner = RunCP2(project_path)
    runner.split_images('image_groups.json')
    runner.run_batches('ExampleFly.cppipe')
    runner.merge_results()
