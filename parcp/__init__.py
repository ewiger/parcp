'''
A simple library to automate running of CellProfiler2 in parallel mode.

@author Yauhen Yakimovich <eugeny.yakimovitch@gmail.com>
'''
import os
import sh
import logging
import textwrap
from glob import glob
from parcp.cpimages import CellProfilerImages


logger = logging.getLogger('parcp')


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


class ParallelCellProfiler(object):

    def __init__(self, project_path):
        self.project = Project(project_path)
        self.cpimages = CellProfilerImages()
        self.result_indexes = list()

    def get_cp2_call(self):
        '''
        Requires a symlink pointing from a home forlder to the actual
        location of CP2. E.g.:

            cd && ln -s ~/dev/pelkmanslab/CellProfiler-2.1.1 CellProfiler2

        '''
        return 'python ' + os.path.expanduser(
            '~/CellProfiler2/CellProfiler.py')

    def load_image_setting(self, image_list_settings_filename):
        '''
        Load JSON file with settings on how to group images into sets -
        batches of files that can be independently processed in parallel.
        '''
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
        if os.path.exists(image_list_settings_filename):
            # Load custom JSON settings file
            logger.info('Parsing grouping settings for images: %s',
                        image_list_settings_filename)
            self.cpimages.parse_settings(image_list_settings_filename)

    def split_images(self):
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

        logger.info('Splitting images into filenames')
        self.cpimages.split_images(images_path, output_path)
        num_of_image_sets = self.cpimages.set_num

        if num_of_image_sets == 0:
            print 'Error, splitting of images failed. No image set were '\
                  'generated.'
            exit(1)
        logger.info('Done. Split input images into %d lists' %
                    num_of_image_sets)

    def get_cp2_batch_command(self, cp_pipeline_file, input_csv_filepath,
                              output_path):
        '''
        Execute CellProfiller2 process in a command line. Pass arguments
        sufficient to process a single batch of images.
        '''
        images_path = self.project.images_path
        command_lines = textwrap.wrap('''
        %(cp2_call)s -b -c -i %(images_path)s -o %(output_path)s \
            --do-not-build --do-not-fetch --pipeline=%(cp_pipeline_file)s \
            --data-file=%(csv_filepath)s -L INFO
        ''' % {
            'cp2_call': self.get_cp2_call(),
            'images_path': images_path,
            'output_path': output_path,
            'cp_pipeline_file': cp_pipeline_file,
            'csv_filepath': input_csv_filepath,
        }, width=210, break_on_hyphens=False, break_long_words=False)
        # return ' \\\n'.join(command_lines)
        return ' '.join(command_lines)

    def exec_command(self, command_code, stdoutlog, stdouterr):
        # print command_code
        args = [arg for arg in command_code.split(' ') if len(arg) > 0]
        print args
        command = sh.Command(args[0])
        result = command(*args[1:], _out=stdoutlog, _err=stdouterr)
        return result

    def run_batch(self, pipeline_filepath, group_index, image_group):
        logger.info('Running cp2 with image group: %s', image_group)
        output_path = os.path.join(self.project.results_path,
                                   str(group_index))
        command_code = self.get_cp2_batch_command(pipeline_filepath,
                                                  image_group, output_path)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        stdoutlog = os.path.join(output_path, 'stdout.log')
        stdouterr = os.path.join(output_path, 'stderr.log')
        result = self.exec_command(command_code, stdoutlog, stdouterr)
        if result.exit_code != 0:
            raise Exception('Failed (exit_code %d) to run: %s' %
                            (result.exit_code, command_code))

    def run_batches(self, pipeline_filename):
        '''
        For each input CSV file found run a CP2 job and produce output.
        Method is not parallel, but useful for debugging and testing
        of next step - merging of results.
        '''
        pipeline_filepath = os.path.join(self.project.path, pipeline_filename)
        image_groups = glob(os.path.join(self.project.image_groups_path,
                            'image_set_*.csv'))
        image_groups = sorted(image_groups)
        for group_index, image_group in enumerate(image_groups):
            # group index is appended to output path of each batch to help
            # differentiate outputs per job in merging of results after the
            # parallel step.
            self.run_batch(pipeline_filepath, group_index, image_group)

    def merge_image_results(self):
        '''
        Special case - merge measurements of image for all results.
        In this case there is no number of the image. The line itself
        represents the number, so the order must be preserved.
        '''
        # TODO: implement me

    def merge_object_results(self, object_name):
        '''
        Merge objects measurements gathered across all result folders into one
        file.
        '''
        logger.info('Merging results for: %s', object_name)
        if object_name == 'Image':
            return self.merge_image_results()
        csv_path_tpl = os.path.join(self.project.results_path, '%d',
                                    object_name + '.csv')
        merged_csv_path = os.path.join(self.project.results_path,
                                       object_name + '.csv')
        # print csv_path_tpl
        logger.info('Writing %s measurements into: %s' %
                    (object_name, merged_csv_path))
        merged_csv = open(merged_csv_path, 'w+')
        merged_header = None
        image_count = 0
        object_count = 0
        for index in self.result_indexes:
            csv_path = csv_path_tpl % index
            # First column is ImageNumber and second is ObjectNumber - take
            # only those to avoid manipulating floats and introducing rounding
            # problems.
            with open(csv_path) as batch_csv:
                header = batch_csv.readline().rstrip()
                if merged_header is not None:
                    assert header == merged_header
                else:
                    merged_header = header
                    merged_csv.writelines([header + "\n"])
                # Manipulate indexes in first and second columns.
                # Don't change the rest.
                prev_img_count = 0
                for line in batch_csv:
                    # Assume comma-sep syntax
                    image_index, object_index, rest = line.rstrip()\
                        .split(',', 2)
                    image_index = int(image_index)
                    object_index = int(object_index)
                    # Merging is sequential, so this a new index
                    object_count += 1
                    if image_index > prev_img_count:
                        prev_img_count = image_index
                        image_count += 1
                    logger.debug(('Writing image %d (global %d):'
                                 ' object %d (global %d)') %
                                 (image_index, image_count,
                                  object_index, object_count))
                    merged_line = ','.join((str(image_count),
                                           str(object_count), rest))
                    merged_csv.writelines([merged_line + "\n"])
        merged_csv.close()
        logger.info('Done merging of: %s', merged_csv_path)

    def merge_results(self):
        '''
        Each job produces output stored as CSV (ExportToSpreadSheet module).
        I.e. we can run only those CP2 pipelines that contain export to CSV
        module.
        '''
        # Assume there is always at least one batch#0. All the CSV files are
        # object names. All CSV in all batches get merged per object.
        # Expected number of objects can be computed as a sum of number of
        # lines in CSV minus one (header line).
        self.result_indexes = [int(result_index) for result_index
                               in os.listdir(self.project.results_path)
                               if result_index.isdigit()]
        # They all are unique and consequent - no value in between
        assert max(self.result_indexes) == len(self.result_indexes) - 1
        # print results

        # Learn object names from 'results/0/*.csv'
        object_names = [os.path.basename(filename)[:-4] for filename
                        in glob(os.path.join(self.project.results_path,
                                '0', '*.csv'))]
        # print object_names
        for object_name in object_names:
            self.merge_object_results(object_name)
