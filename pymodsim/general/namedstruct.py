
def makestruct(name, fields):
    fields = fields.split()
    import textwrap
    template = textwrap.dedent("""\
    class {name}(object):
        __slots__ = {fields!r}
        def __init__(self, {args}):
            {self_fields} = {args}
        def __getitem__(self, idx): 
            return getattr(self, fields[idx])
        def __getstate__(self):
            return {{key: getattr(self, key) for key in self.__slots__}}
        def __setstate__(self, state):
            {self_fields} = {set_keys}
    """).format(
        name=name,
        fields=fields,
        args=','.join(fields), 
        self_fields=','.join('self.' + f for f in fields),
        set_keys = ','.join('state["' + f + '"]' for f in fields))
    d = {'fields': fields}
    exec(template, d)
    return d[name]