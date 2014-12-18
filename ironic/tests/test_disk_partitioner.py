# Copyright 2014 Red Hat, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import fixtures
import mock
from oslo_concurrency import processutils
from testtools.matchers import HasLength

from ironic.common import disk_partitioner
from ironic.common import exception
from ironic.common import utils
from ironic.tests import base


class DiskPartitionerTestCase(base.TestCase):

    def setUp(self):
        super(DiskPartitionerTestCase, self).setUp()

        def noop(*args, **kwargs):
            pass

        self.useFixture(fixtures.MonkeyPatch('eventlet.greenthread.sleep',
                                             noop))

        self.dp = disk_partitioner.DiskPartitioner('/dev/fake')
        self.fake_parts = [(1, {'bootable': False,
                                'fs_type': 'fake-fs-type',
                                'type': 'fake-type',
                                'size': 1}),
                           (2, {'bootable': True,
                                'fs_type': 'fake-fs-type',
                                'type': 'fake-type',
                                'size': 1})]

    def test_add_partition(self):
        self.dp.add_partition(1024)
        self.dp.add_partition(512, fs_type='linux-swap')
        self.dp.add_partition(2048, bootable=True)
        expected = [(1, {'bootable': False,
                         'fs_type': '',
                         'type': 'primary',
                         'size': 1024}),
                    (2, {'bootable': False,
                         'fs_type': 'linux-swap',
                         'type': 'primary',
                         'size': 512}),
                    (3, {'bootable': True,
                         'fs_type': '',
                         'type': 'primary',
                         'size': 2048})]
        partitions = [(n, p) for n, p in self.dp.get_partitions()]
        self.assertThat(partitions, HasLength(3))
        self.assertEqual(expected, partitions)

    @mock.patch.object(disk_partitioner, '_list_devfs')
    @mock.patch.object(disk_partitioner.DiskPartitioner, '_exec')
    @mock.patch.object(utils, 'execute')
    def test_commit(self, mock_utils_exc, mock_disk_partitioner_exec,
                    mock_list_devfs):
        mock_list_devfs.return_value = ['/dev/fake1', '/dev/fake2']
        with mock.patch.object(self.dp, 'get_partitions') as mock_gp:
            mock_gp.return_value = self.fake_parts
            mock_utils_exc.return_value = (None, None)
            self.dp.commit()

        mock_disk_partitioner_exec.assert_called_once_with('mklabel', 'msdos',
            'mkpart', 'fake-type', 'fake-fs-type', '1', '2',
            'mkpart', 'fake-type', 'fake-fs-type', '2', '3',
            'set', '2', 'boot', 'on')

        expected = [mock.call('fuser', '/dev/fake', run_as_root=True,
                              check_exit_code=[0, 1]),
                    mock.call('partprobe', '/dev/fake', run_as_root=True,
                              check_exit_code=[0])]
        self.assertEqual(expected, mock_utils_exc.mock_calls)
        self.assertEqual(2, mock_utils_exc.call_count)
        mock_list_devfs.assert_called_once_with('/dev/fake')

    @mock.patch.object(disk_partitioner, '_list_devfs')
    @mock.patch.object(disk_partitioner.DiskPartitioner, '_exec')
    @mock.patch.object(utils, 'execute')
    def test_commit_with_device_is_busy_once(self, mock_utils_exc,
                                             mock_disk_partitioner_exec,
                                             mock_list_devfs):
        mock_list_devfs.return_value = ['/dev/fake1', '/dev/fake2']
        fuser_outputs = [("/dev/fake: 10000 10001", None),  # fuser
                         (None, None),  # fuser
                         (None, None)]  # partprobe

        with mock.patch.object(self.dp, 'get_partitions') as mock_gp:
            mock_gp.return_value = self.fake_parts
            mock_utils_exc.side_effect = fuser_outputs
            self.dp.commit()

        mock_disk_partitioner_exec.assert_called_once_with('mklabel', 'msdos',
            'mkpart', 'fake-type', 'fake-fs-type', '1', '2',
            'mkpart', 'fake-type', 'fake-fs-type', '2', '3',
            'set', '2', 'boot', 'on')
        expected = [mock.call('fuser', '/dev/fake', run_as_root=True,
                              check_exit_code=[0, 1]),
                    mock.call('fuser', '/dev/fake', run_as_root=True,
                              check_exit_code=[0, 1]),
                    mock.call('partprobe', '/dev/fake', run_as_root=True,
                              check_exit_code=[0])]
        self.assertEqual(expected, mock_utils_exc.mock_calls)
        self.assertEqual(3, mock_utils_exc.call_count)
        mock_list_devfs.assert_called_once_with('/dev/fake')

    @mock.patch.object(disk_partitioner, '_list_devfs')
    @mock.patch.object(disk_partitioner.DiskPartitioner, '_exec')
    @mock.patch.object(utils, 'execute')
    def test_commit_with_device_is_always_busy(self, mock_utils_exc,
                                               mock_disk_partitioner_exec,
                                               mock_list_devfs):
        with mock.patch.object(self.dp, 'get_partitions') as mock_gp:
            mock_gp.return_value = self.fake_parts
            mock_utils_exc.return_value = ("/dev/fake: 10000 10001", None)
            self.assertRaises(exception.InstanceDeployFailure, self.dp.commit)

        mock_disk_partitioner_exec.assert_called_once_with('mklabel', 'msdos',
            'mkpart', 'fake-type', 'fake-fs-type', '1', '2',
            'mkpart', 'fake-type', 'fake-fs-type', '2', '3',
            'set', '2', 'boot', 'on')
        mock_utils_exc.assert_called_with('fuser', '/dev/fake',
            run_as_root=True, check_exit_code=[0, 1])
        self.assertEqual(20, mock_utils_exc.call_count)
        self.assertFalse(mock_list_devfs.called)

    @mock.patch.object(disk_partitioner, '_list_devfs')
    @mock.patch.object(disk_partitioner.DiskPartitioner, '_exec')
    @mock.patch.object(utils, 'execute')
    def test_commit_with_device_disconnected(self, mock_utils_exc,
                                             mock_disk_partitioner_exec,
                                             mock_list_devfs):
        with mock.patch.object(self.dp, 'get_partitions') as mock_gp:
            mock_gp.return_value = self.fake_parts
            mock_utils_exc.return_value = (None, "Specified filename /dev/fake"
                                                 " does not exist.")
            self.assertRaises(exception.InstanceDeployFailure, self.dp.commit)

        mock_disk_partitioner_exec.assert_called_once_with('mklabel', 'msdos',
            'mkpart', 'fake-type', 'fake-fs-type', '1', '2',
            'mkpart', 'fake-type', 'fake-fs-type', '2', '3',
            'set', '2', 'boot', 'on')
        mock_utils_exc.assert_called_with('fuser', '/dev/fake',
            run_as_root=True, check_exit_code=[0, 1])
        self.assertEqual(20, mock_utils_exc.call_count)
        self.assertFalse(mock_list_devfs.called)

    @mock.patch.object(disk_partitioner, '_list_devfs')
    @mock.patch.object(disk_partitioner.DiskPartitioner, '_exec')
    @mock.patch.object(utils, 'execute')
    def test_commit_with_device_partprobe_fail_once(self, mock_utils_exc,
                                                   mock_disk_partitioner_exec,
                                                   mock_list_devfs):
        mock_list_devfs.return_value = ['/dev/fake1', '/dev/fake2']
        exc_outputs = [
            (None, None),  # fuser success
            processutils.ProcessExecutionError(''),  # partprobe fail once
            (None, None),  # fuser success
            (None, None)   # partprobe success
        ]

        with mock.patch.object(self.dp, 'get_partitions') as mock_gp:
            mock_gp.return_value = self.fake_parts
            mock_utils_exc.side_effect = exc_outputs
            self.dp.commit()

        mock_disk_partitioner_exec.assert_called_once_with('mklabel', 'msdos',
            'mkpart', 'fake-type', 'fake-fs-type', '1', '2',
            'mkpart', 'fake-type', 'fake-fs-type', '2', '3',
            'set', '2', 'boot', 'on')
        expected = [mock.call('fuser', '/dev/fake', run_as_root=True,
                              check_exit_code=[0, 1]),
                    mock.call('partprobe', '/dev/fake', run_as_root=True,
                              check_exit_code=[0]),
                    mock.call('fuser', '/dev/fake', run_as_root=True,
                              check_exit_code=[0, 1]),
                    mock.call('partprobe', '/dev/fake', run_as_root=True,
                              check_exit_code=[0])]
        self.assertEqual(expected, mock_utils_exc.mock_calls)
        self.assertEqual(4, mock_utils_exc.call_count)
        mock_list_devfs.assert_called_once_with('/dev/fake')


@mock.patch.object(utils, 'execute')
class ListPartitionsTestCase(base.TestCase):

    def test_correct(self, execute_mock):
        output = """
BYT;
/dev/sda:500107862016B:scsi:512:4096:msdos:ATA HGST HTS725050A7:;
1:1.00MiB:501MiB:500MiB:ext4::boot;
2:501MiB:476940MiB:476439MiB:::;
"""
        expected = [
            {'start': 1, 'end': 501, 'size': 500,
             'filesystem': 'ext4', 'flags': 'boot'},
            {'start': 501, 'end': 476940, 'size': 476439,
             'filesystem': '', 'flags': ''},
        ]
        execute_mock.return_value = (output, '')
        result = disk_partitioner.list_partitions('/dev/fake')
        self.assertEqual(expected, result)
        execute_mock.assert_called_once_with(
            'parted', '-s', '-m', '/dev/fake', 'unit', 'MiB', 'print',
            use_standard_locale=True)

    @mock.patch.object(disk_partitioner.LOG, 'warn')
    def test_incorrect(self, log_mock, execute_mock):
        output = """
BYT;
/dev/sda:500107862016B:scsi:512:4096:msdos:ATA HGST HTS725050A7:;
1:XX1076MiB:---:524MiB:ext4::boot;
"""
        execute_mock.return_value = (output, '')
        self.assertEqual([], disk_partitioner.list_partitions('/dev/fake'))
        self.assertEqual(1, log_mock.call_count)
