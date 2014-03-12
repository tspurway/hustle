import os
from disco.ddfs import DDFS
from disco.core import Disco


def guess_settings():
    for settings_file in (os.path.expanduser('~/.hustle'),
                          '/etc/hustle/settings.yaml'):
        if os.path.exists(settings_file):
            return settings_file
    return ''


defaults = {
    'settings_file': guess_settings(),
    'server': 'disco://localhost',
    'nest': False,
    'dump': False,
    'worker_class': 'disco.worker.classic.worker.Worker',
    'partition': 16,
    'history_size': 1000
}

overrides = {}


class Settings(dict):
    def __init__(self, *args, **kwargs):
        # load the defaults
        super(Settings, self).update(defaults)

        # override with the settings file
        path = kwargs.get('settings_file') or self['settings_file']
        if path and os.path.exists(path):
            try:
                import yaml
                self.update(yaml.load(open(path)))
            except:
                pass  # if ya can't ya can't

        # final overrides
        super(Settings, self).update(overrides)
        super(Settings, self).__init__(*args, **kwargs)

        # set up ddfs and disco
        if not self['server'].startswith('disco://'):
            self['server'] = 'disco://' + self['server']

        if 'ddfs' not in self:
            self['ddfs'] = DDFS(self['server'])
        self['server'] = Disco(self['server'])

        # set up worker
        if 'worker' not in self:
            worker_mod, _, worker_class = self['worker_class'].rpartition('.')
            mod = __import__(worker_mod, {}, {}, worker_mod)
            self['worker'] = getattr(mod, worker_class)()
