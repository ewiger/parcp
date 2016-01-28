import os
import re
import io
import csv
# pip install PyYaml
import yaml
import fnmatch


class NoImageFilesFound(Exception):
    '''
    Raised as soon as no images matching expected filters found.
    '''


class CellProfilerImages(object):
    '''
    Handling data loading for CellProfiler:
    - Load settings telling how to parse image filenames and split those files
      into groups for batching.
    - File lists a saved as CSV files acceptable by LoadData module of
      CellProfiler2.
    '''

    def __init__(self, settings=None):
        self.set_num = 0
        self.settings = dict() if not settings else settings
        self.saved_csv_files = list()
        self.__group_key_map = None

    @property
    def group_by_field(self):
        '''Group images by 'group by' field, e.g. image channel.'''
        return self.settings.get('group_by_field', 'Channel')

    @property
    def image_set_size_per_batch(self):
        return int(self.settings.get(
            'image_set_size_per_batch', '10'))

    @property
    def group_key_mapping(self):
        if self.__group_key_map is None:
            self.__group_key_map = dict()
            self.__group_key_map.update(
                self.settings.get(
                    'group_key_map',
                    # Default mapping. Can be changed by user.
                    {
                        '0': 'OrigBlue',
                        '1': 'OrigGreen',
                        '2': 'OrigRed',
                        '3': 'OrigFarRed',
                    }
                )
            )
        return self.__group_key_map

    def get_object_name(self, group_key):
        for key in self.group_key_mapping:
            if key in group_key:
                return self.group_key_mapping[key]
        raise KeyError('Check settings. Failed to map object by group key %s'
                       % group_key)

    def get_image_files(self, image_files_path):
        '''
        Return a list of parsed metadata values obtained from the filename.
        Each value is a dictionary of key-value pairs.
        '''
        # Get image file name filter from settings. Look for .png or .tif by
        # default
        image_name_expr = '.*(?P<Channel>d\d)(\.png|\.tiff?)'
        if 'image_name_filter_fn' in self.settings:
            image_name_expr = fnmatch.translate(
                self.settings['image_name_filter_fn'])
        elif 'image_name_filter_re' in self.settings:
            image_name_expr = self.settings['image_name_filter_re']
        # Filter for matching names only
        filename_regex_obj = re.compile(image_name_expr, re.IGNORECASE)
        image_files = list()
        for filename in os.listdir(image_files_path):
            match = filename_regex_obj.search(filename)
            if not match:
                continue
            metadata = dict()
            metadata.update(match.groupdict())
            metadata['filename'] = filename
            image_files.append(metadata)
        return image_files

    def parse_settings(self, settings_filepath):
        data = False
        with open(settings_filepath) as stream:
            data = yaml.load(stream)
        if not data:
            raise Exception('Empty settings file or malformed JSON')
        self.settings.update(data)

    def split_images(self, image_files_path, output_path):
        # Output path for produced lists of images.
        if 'relative_output_path' in self.settings:
            file_lists_path = os.path.join(
                output_path,
                self.settings['relative_output_path'],
            )
        else:
            file_lists_path = output_path
        assert os.path.exists(file_lists_path)
        # Get image lists.
        image_files = self.get_image_files(image_files_path)
        if len(image_files) == 0:
            raise NoImageFilesFound()

        sorted_image_files = sorted(image_files,
                                    key=lambda k: k[self.group_by_field])
        image_groups = dict()
        for metadata in sorted_image_files:
            group_key = metadata[self.group_by_field]
            if group_key not in image_groups:
                image_groups[group_key] = list()
            image_groups[group_key].append(metadata)
        for group_key in image_groups:
            image_groups[group_key] = sorted(
                image_groups[group_key],
                key=lambda k: k['filename'],
            )
        # Guarantee there is at least one group and each group has the same
        # size.
        group_keys = image_groups.keys()
        group_keys = sorted(group_keys)
        number_of_groups = len(group_keys)
        assert number_of_groups > 0
        image_group_size = len(image_groups[group_keys[0]])
        assert all([len(image_groups[key]) == image_group_size
                   for key in image_groups])
        # Split each group into a series of sets, such that each value is the
        # same image different only by 'group by' value.
        image_set = list()
        for step in range(image_group_size):
            image_entry = list()
            shared_image_name = image_groups[group_keys[0]][step]['filename']\
                .replace(
                    image_groups[group_keys[0]][step][self.group_by_field],
                    '',
                )
            for group_key in group_keys:
                metadata = image_groups[group_key][step]
                # Filename should be the same, i.e. correctly aligned by
                # grouping.
                assert shared_image_name == metadata['filename']\
                    .replace(metadata[self.group_by_field], '', 1)
                image_entry.append(image_groups[group_key][step])
            image_set.append(image_entry)
            if len(image_set) >= self.image_set_size_per_batch:
                self.save_as_csv_list(file_lists_path, image_set)
                image_set = list()
        if len(image_set) > 0:
            self.save_as_csv_list(file_lists_path, image_set)

    def next_csv_filename(self):
        csv_template = self.settings.get('csv_template',
                                         'image_set_%(set_num)d.csv')
        csv_filename = csv_template % {
            'set_num': self.set_num,
        }
        self.set_num += 1
        return csv_filename

    def save_as_csv_list(self, file_lists_path, image_set):
        csv_filename = os.path.join(file_lists_path,
                                    self.next_csv_filename())
        self.saved_csv_files.append(csv_filename)
        with io.open(csv_filename, mode='wb') as f:
            fieldnames = image_set[0][0].keys()
            fieldnames.remove('filename')
            fieldnames.insert(0, 'filename')
            fieldmap = dict()
            rows = list()
            has_header = False
            header = list()
            for image_entry in image_set:
                row = dict()
                for object_num, metadata in enumerate(image_entry):
                    # Get object name e.g. OrigBlue.
                    objectname = self.get_object_name(
                        metadata[self.group_by_field])
                    if not has_header:
                        for fieldname in fieldnames:
                            if fieldname == 'filename':
                                mapped_fieldname = 'Image_FileName_%s' % (
                                    objectname)
                            else:
                                mapped_fieldname = 'Metadata_%s_%s' % (
                                    fieldname.title(), objectname)
                            header.append(mapped_fieldname)
                            object_key = '%s_%d' % (fieldname, object_num)
                            fieldmap[object_key] = mapped_fieldname
                    for fieldname in metadata:
                        object_key = '%s_%d' % (fieldname, object_num)
                        row[fieldmap[object_key]] = metadata[fieldname]
                if not has_header:
                    has_header = True
                rows.append(row)
            # Finally, write rows to disk.
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
