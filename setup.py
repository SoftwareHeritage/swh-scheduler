from setuptools import setup


def parse_requirements():
    requirements = []
    with open('requirements.txt') as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            requirements.append(line)

    return requirements


setup(
    name='swh.scheduler',
    description='Software Heritage Scheduler',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DSCH/',
    packages=[
        'swh.scheduler', 'swh.scheduler.celery_backend', 'swh.scheduler.tests'
    ],
    scripts=[],   # scripts to package
    install_requires=parse_requirements(),
    entry_points='''
        [console_scripts]
        swh-scheduler=swh.scheduler.cli:cli
    ''',
    setup_requires=['vcversioner'],
    vcversioner={},
    include_package_data=True,
)
