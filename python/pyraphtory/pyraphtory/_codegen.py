from keyword import iskeyword
from pyraphtory._docstring import convert_docstring


def clean_identifier(name: str):
    if iskeyword(name):
        return str(name + "_")
    else:
        return str(name)


def build_method(name, method, jpype=False):
    params = [clean_identifier(name) for name in method.parameters()]
    name = clean_identifier(name)
    java_name = clean_identifier(method.name()) if jpype else method.name()
    implicits = [clean_identifier(name) for name in method.implicits()]
    nargs = method.n()
    varargs = method.varargs()
    if varargs:
        varparam = params.pop()
    defaults = method.defaults()
    required = max(defaults.keys(), default=nargs)

    args = ["self"]
    args.extend(p + f'=DefaultValue("{defaults[i]}")' if i in defaults
                else (p + '=None' if i > required
                      else p)
                for i, p in enumerate(params))
    if varargs:
        args.append(f"*{varparam}")
    if implicits:
        args.append("_implicits=()")
    args = ", ".join(args)

    lines = [f"def {name}({args}):"]
    lines.append(f'    """{convert_docstring(method.docs())}"""')
    if implicits:
        lines.append(f"    if len(_implicits) < {len(implicits)}:")
        lines.append(f"        raise RuntimeError('missing implicit arguments')")
        lines.append(f"    if len(_implicits) > {len(implicits)}:")
        lines.append(f"        raise RuntimeError('too many implicit arguments')")
        for i, p in enumerate(implicits):
            lines.append(f"    {p} = to_jvm(_implicits[{i}])")
    lines.extend(f"    {p} = _check_default(self._jvm_object, {p})" if i in defaults else
                 f"    {p} = to_jvm({p})" for i, p in enumerate(params))
    if varargs:
        lines.append(f"    {varparam} = make_varargs(to_jvm(list({varparam})))")
        params.append(varparam)
    lines.append(f"    return to_python(getattr(self._jvm_object, '{java_name}')({', '.join(p for p in params + implicits)}))")
    return "\n".join(lines)
