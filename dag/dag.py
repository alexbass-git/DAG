from inspect import isfunction, signature, getsource
import re


def dag(cls):
    nodes = {}
    for name, method in cls.__dict__.items():
        dag_attr = getattr(method, '_dag', None)
        if dag_attr is not None:
            params = signature(method).parameters
            assert len(params) == 1, 'Node function must have a single parameter'
            nodes.setdefault(name, [[], None, None])[1] = method
            sname = next(iter(params))
            source = getsource(method)
            ptrn = re.compile(rf'(?<![\w]){sname}\.([a-zA-Z_]\w*)')
            deps = set(ptrn.findall(source))
            for dep in deps:
                nodes.setdefault(dep, [[], None, None])[0].append(name)
    for name, node in nodes.items():
        if node[1] is not None:
            delattr(cls, name)

    class DAG(cls):
        _nodes = nodes

        def __getattr__(self, item):
            try:
                get_method = DAG._nodes[item][1]
            except KeyError:
                return cls.__getattr__(self, item)
            res = get_method(self)
            cls.__setattr__(self, item, res)
            return res

        def __delattr__(self, key):
            try:
                node = DAG._nodes[key]
            except KeyError:
                return cls.__delattr__(self, key)
            try:
                cls.__delattr__(self, key)
            except AttributeError:
                return

            for dep in node[0]:
                try:
                    delattr(self, dep)
                except AttributeError:
                    pass

        def __setattr__(self, key, value):
            node = DAG._nodes.get(key, None)
            if node is None:
                return cls.__setattr__(self, key, value)
            deps, get_method, set_method = node[:3]
            if set_method is not None:
                return set_method(value)
            if get_method is None:
                try:
                    object.__getattribute__(self, key)
                    for dep in deps:
                        delattr(self, dep)
                except AttributeError:
                    pass
                return cls.__setattr__(self, key, value)
            raise AttributeError(f'Attribute {value} is not settable')

    return DAG


def dag_node(*_):
    def deco(method):
        assert isfunction(method), '@dag_node must be a method'
        method._dag = {}
        return method

    return deco
