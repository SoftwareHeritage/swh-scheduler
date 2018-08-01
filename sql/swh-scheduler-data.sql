insert into task_type(
       type,
       description,
       backend_name,
       default_interval, min_interval, max_interval, backoff_factor,
       max_queue_length)
values (
       'swh-loader-mount-dump-and-load-svn-repository',
       'Loading svn repositories from svn dump',
       'swh.loader.svn.tasks.MountAndLoadSvnRepositoryTsk',
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
       'origin-load-hg',
       'Loading mercurial repository swh-loader-mercurial',
       'swh.loader.mercurial.tasks.LoadMercurialTsk',
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
       'swh.loader.mercurial.tasks.LoadArchiveMercurialTsk',
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
       '64 days', 2, 100000);

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
       'swh.lister.pypi.tasks.PyPiListerTask',
       '90 days',
       '90 days',
       '90 days', 1);

