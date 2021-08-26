insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'load-svn-from-archive',
       'Loading svn repositories from svn dump',
       'swh.loader.svn.tasks.MountAndLoadSvnRepository',
       '1 day', '1 day', '1 day', 1,
       1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'load-svn',
       'Create dump of a remote svn repository, mount it and load it',
       'swh.loader.svn.tasks.DumpMountAndLoadSvnRepository',
       '1 day', '1 day', '1 day', 1,
       1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       num_retries, max_queue_length)
values (
       'check-deposit',
       'Pre-checking deposit step before loading into swh archive',
       'swh.deposit.loader.tasks.ChecksDepositTsk',
       '1 day', '1 day', '1 day', 1, 3, 1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'cook-vault-bundle',
       'Cook a Vault bundle',
       'swh.vault.cooking_tasks.SWHCookingTask',
       '1 day', '1 day', '1 day', 1,
 10000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'load-hg',
       'Loading mercurial repository swh-loader-mercurial',
       'swh.loader.mercurial.tasks.LoadMercurial',
       '1 day', '1 day', '1 day', 1,
       1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'load-hg-from-archive',
       'Loading archive mercurial repository swh-loader-mercurial',
       'swh.loader.mercurial.tasks.LoadArchiveMercurial',
       '1 day', '1 day', '1 day', 1,
       1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'load-git',
       'Update an origin of type git',
       'swh.loader.git.tasks.UpdateGitRepository',
       '64 days',
       '12:00:00',
       '64 days', 2, 5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-mimetype',
       'Mimetype indexer task',
       'swh.indexer.tasks.ContentMimetype',
       '1 day', '12:00:00', '1 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-mimetype-for-range',
       'Mimetype Range indexer task',
       'swh.indexer.tasks.ContentRangeMimetype',
       '1 day', '12:00:00', '1 days', 2,
       5000);


insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-fossology-license',
       'Fossology license indexer task',
       'swh.indexer.tasks.ContentFossologyLicense',
       '1 day', '12:00:00', '1 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-fossology-license-for-partition',
       'Fossology license partition indexer task',
       'swh.indexer.tasks.ContentFossologyLicensePartition',
       '1 day', '12:00:00', '1 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-origin-head',
       'Origin Head indexer task',
       'swh.indexer.tasks.OriginHead',
       '1 day', '12:00:00', '1 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-revision-metadata',
       'Revision Metadata indexer task',
       'swh.indexer.tasks.RevisionMetadata',
       '1 day', '12:00:00', '1 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'index-origin-metadata',
       'Origin Metadata indexer task',
       'swh.indexer.tasks.OriginMetadata',
       '1 day', '12:00:00', '1 days', 2,
       20000);
