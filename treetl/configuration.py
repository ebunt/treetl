
from io import StringIO
from collections import namedtuple
from configparser import ConfigParser, ExtendedInterpolation


class ConfigReader(ConfigParser):
    def __init__(self, filename=None, config_str=None, case_sensitive=False, **kwargs):
        super(ConfigReader, self).__init__(**kwargs)
        self._interpolation = ExtendedInterpolation()
        if case_sensitive:
            self.optionxform = str

        self.filename = filename
        self.conversions = { }
        self.case_sensitive = case_sensitive

        if filename is not None:
            self.read(filename)

        # system issues with ini as strings
        if config_str is not None:
            try:
                # for windows
                if isinstance(config_str, str):
                    self.read_file(StringIO(config_str))
                else:
                    if hasattr(config_str, 'decode'):
                        self.read_string(config_str.decode('utf-8'))
                    else:
                        self.read_string(config_str)
            except Exception as e:
                # for mac
                self.read_string(unicode(config_str))

    def add_conversions(self, section=None, **kwargs):
        def name(opt):
            return '{}_{}'.format(section, opt)

        def get_converter(v):
            if v == bool:
                return lambda val: val.lower() == 'true'
            elif isinstance(v, type):
                return lambda val: v(s)
            else:
                return v

        next_name = name if section is not None else lambda n: n
        if kwargs:
            for k, v in kwargs.items():
                self.conversions[next_name(k)] = get_converter(v)

        return self

    def get(self, section, option, **kwargs):
        # parent calls self.get with various parameters, but derived never does
        # move it up along the chain and don't convert if raw is passed
        if kwargs:
            return super(ConfigReader, self).get(section, option, **kwargs)

        full_n = '{}_{}'.format(section, option)
        conv = self.conversions.get(
            full_n,
            self.conversions.get(option, lambda s: s)
        )

        return conv(super(ConfigReader, self).get(section, option))

    def get_dict(self, section):
        return { opt: self.get(section, opt) for opt in self.options(section) }

    def __getattr__(self, item):
        if item in self.sections():
            sect = namedtuple(item, self.options(item))
            res = sect(*[ self.get(item, opt) for opt in self.options(item) ])
            return res
