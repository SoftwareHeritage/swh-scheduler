insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'swh-loader-mount-dump-and-load-svn-repository',
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
       'origin-update-svn',
       'Create dump of a remote svn repository, mount it and load it',
       'swh.loader.svn.tasks.DumpMountAndLoadSvnRepository',
       '1 day', '1 day', '1 day', 1,
       1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       num_retries,
       max_queue_length)
values (
       'swh-deposit-archive-loading',
       'Loading deposit archive into swh through swh-loader-tar',
       'swh.deposit.loader.tasks.LoadDepositArchiveTsk',
       '1 day', '1 day', '1 day', 1, 3, 1000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       num_retries, max_queue_length)
values (
       'swh-deposit-archive-checks',
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
       'swh-vault-cooking',
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
       'origin-update-hg',
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
       'origin-load-archive-hg',
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
       'origin-update-git',
       'Update an origin of type git',
       'swh.loader.git.tasks.UpdateGitRepository',
       '64 days',
       '12:00:00',
       '64 days', 2, 5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-bitbucket-incremental',
       'Incrementally list BitBucket',
       'swh.lister.bitbucket.tasks.IncrementalBitBucketLister',
       '1 day',
       '1 day',
       '1 day', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-bitbucket-full',
       'Full update of Bitbucket repos list',
       'swh.lister.bitbucket.tasks.FullBitBucketRelister',
       '90 days',
       '90 days',
       '90 days', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-github-incremental',
       'Incrementally list GitHub',
       'swh.lister.github.tasks.IncrementalGitHubLister',
       '1 day',
       '1 day',
       '1 day', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-github-full',
       'Full update of GitHub repos list',
       'swh.lister.github.tasks.FullGitHubRelister',
       '90 days',
       '90 days',
       '90 days', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-debian',
       'List a Debian distribution',
       'swh.lister.debian.tasks.DebianListerTask',
       '1 day',
       '1 day',
       '1 day', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-gitlab-incremental',
       'Incrementally list a Gitlab instance',
       'swh.lister.gitlab.tasks.IncrementalGitLabLister',
       '1 day',
       '1 day',
       '1 day', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-gitlab-full',
       'Full update of a Gitlab instance''s repos list',
       'swh.lister.gitlab.tasks.FullGitLabRelister',
       '90 days',
       '90 days',
       '90 days', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'swh-lister-pypi',
       'Full pypi lister',
       'swh.lister.pypi.tasks.PyPIListerTask',
       '1 days',
       '1 days',
       '1 days', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'origin-update-pypi',
       'Load Pypi origin',
       'swh.loader.pypi.tasks.LoadPyPI',
       '64 days', '12:00:00', '64 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'indexer_mimetype',
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
       'indexer_range_mimetype',
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
       'indexer_fossology_license',
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
       'indexer_range_fossology_license',
       'Fossology license range indexer task',
       'swh.indexer.tasks.ContentRangeFossologyLicense',
       '1 day', '12:00:00', '1 days', 2,
       5000);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'indexer_origin_head',
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
       'indexer_revision_metadata',
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
       'indexer_origin_metadata',
       'Origin Metadata indexer task',
       'swh.indexer.tasks.OriginMetadata',
       '1 day', '12:00:00', '1 days', 2,
       20000);
