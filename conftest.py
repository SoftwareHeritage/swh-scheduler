from hypothesis import settings

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("fast", max_examples=5, deadline=5000)
settings.register_profile("slow", max_examples=20, deadline=5000)

# Modules that should not be loaded by --doctest-modules
collect_ignore = [
    # NotImplementedError: save_group is not supported by this backend.
    'swh/scheduler/tests/tasks.py',
    # OSError: Configuration file must be defined
    'swh/scheduler/api/wsgi.py',
]
