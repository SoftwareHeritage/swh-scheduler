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
       num_retries,
       max_queue_length)
values (
       'load-deposit-from-archive',
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
       'check-deposit-archive',
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
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'list-bitbucket-incremental',
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
       'list-bitbucket-full',
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
       'list-github-incremental',
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
       'list-github-full',
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
       'list-debian-distribution',
       'List a Debian distribution',
       'swh.lister.debian.tasks.DebianListerTask',
       '1 day',
       '1 day',
       '1 day', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length, num_retries, retry_delay)
values (
       'load-debian-package',
       'Load a Debian package',
       'swh.loader.debian.tasks.LoadDebianPackage',
       NULL, NULL, NULL, NULL,
       5000, 5, '1 hour');

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'list-gitlab-incremental',
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
       'list-gitlab-full',
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
       'list-pypi',
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
       'load-pypi',
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
       'index-fossology-license-for-range',
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

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor)
values (
       'list-npm-full',
       'Full npm lister',
       'swh.lister.npm.tasks.NpmListerTask',
       '1 week',
       '1 week',
       '1 week', 1);

insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'load-npm',
       'Load npm origin',
       'swh.loader.npm.tasks.LoadNpm',
       '64 days', '12:00:00', '64 days', 2,
       5000);

--- For backward compatibility with previous task names
--- TODO: remove this once all swh components have been migrated to use the
---       new task names

create or replace function swh_add_backward_compatible_task_name(
      old_task_name text, new_task_name text)
  returns void
  language sql
as $$
  insert into task_type (
      type, description, backend_name,
      default_interval, min_interval,
      max_interval, backoff_factor,
      max_queue_length, num_retries,
      retry_delay)
  (select old_task_name, description,
          backend_name, default_interval,
          min_interval, max_interval, backoff_factor,
          max_queue_length, num_retries, retry_delay
   from task_type where type = new_task_name);
$$;

select swh_add_backward_compatible_task_name('swh-loader-mount-dump-and-load-svn-repository',
                                             'load-svn-from-archive');

select swh_add_backward_compatible_task_name('origin-update-svn',
                                             'load-svn');

select swh_add_backward_compatible_task_name('swh-deposit-archive-loading',
                                             'load-deposit-from-archive');

select swh_add_backward_compatible_task_name('swh-deposit-archive-checks',
                                             'check-deposit-archive');

select swh_add_backward_compatible_task_name('swh-vault-cooking',
                                             'cook-vault-bundle');

select swh_add_backward_compatible_task_name('origin-update-hg',
                                             'load-hg');

select swh_add_backward_compatible_task_name('origin-load-archive-hg',
                                             'load-hg-from-archive');

select swh_add_backward_compatible_task_name('origin-update-git',
                                             'load-git');

select swh_add_backward_compatible_task_name('swh-lister-bitbucket-incremental',
                                             'list-bitbucket-incremental');

select swh_add_backward_compatible_task_name('swh-lister-bitbucket-full',
                                             'list-bitbucket-full');

select swh_add_backward_compatible_task_name('swh-lister-github-incremental',
                                             'list-github-incremental');

select swh_add_backward_compatible_task_name('swh-lister-github-full',
                                             'list-github-full');

select swh_add_backward_compatible_task_name('swh-lister-debian',
                                             'list-debian-distribution');

select swh_add_backward_compatible_task_name('load-deb-package',
                                             'load-debian-package');

select swh_add_backward_compatible_task_name('swh-lister-gitlab-incremental',
                                             'list-gitlab-incremental');

select swh_add_backward_compatible_task_name('swh-lister-gitlab-full',
                                             'list-gitlab-full');

select swh_add_backward_compatible_task_name('swh-lister-pypi',
                                             'list-pypi');

select swh_add_backward_compatible_task_name('origin-update-pypi',
                                             'load-pypi');

select swh_add_backward_compatible_task_name('indexer_mimetype',
                                             'index-mimetype');

select swh_add_backward_compatible_task_name('indexer_range_mimetype',
                                             'index-mimetype-for-range');

select swh_add_backward_compatible_task_name('indexer_fossology_license',
                                             'index-fossology-license');

select swh_add_backward_compatible_task_name('indexer_range_fossology_license',
                                             'index-fossology-license-for-range');

select swh_add_backward_compatible_task_name('indexer_origin_head',
                                             'index-origin-head');

select swh_add_backward_compatible_task_name('indexer_revision_metadata',
                                             'index-revision-metadata');

select swh_add_backward_compatible_task_name('indexer_origin_metadata',
                                             'index-origin-metadata');

drop function swh_add_backward_compatible_task_name(text, text);
