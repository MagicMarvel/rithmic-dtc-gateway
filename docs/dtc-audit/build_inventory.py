"""Generate an independent ABI inventory from the downloaded official header."""
from pathlib import Path
import re, subprocess, json

root = Path(__file__).resolve().parent
header = (root / 'DTCProtocol.h').read_text(encoding='utf-8-sig')
source = (root.parents[1] / 'src/dtc.rs').read_text(encoding='utf-8')
constants = re.findall(r'^\s*const uint16_t (\w+) = (\d+);(.*)', header, re.M)
implemented = {int(v): n for n, v in re.findall(r'pub const (\w+): u16 = (\d+);', source)}
structs = []
cpp = ['#include <climits>', '#include <cstddef>', '#include <cstdio>', '#include "DTCProtocol.h"', 'int main() {']
for match in re.finditer(r'\bstruct (s_\w+)(?://[^\n]*)?\s*\{(.*?)\n\t\};', header, re.S):
    name, body = match.groups()
    mt = re.search(r'MESSAGE_TYPE = (\w+);', body)
    if not mt:
        continue
    fields = []
    for line in body.splitlines():
        field = re.match(r'\s*([\w:]+)\s+(\w+)(\[[^\]]+\])?\s*(?:=\s*(.*?))?;', line)
        if field and field[2] != 'operator':
            typ, member, array, default = field.groups()
            fields.append(dict(type=typ, name=member, array=array or '', default=default or 'union alias'))
            cpp.append(f'printf("{mt[1]}\t{name}\t{member}\t%zu\t%zu\\n", offsetof(DTC::{name}, {member}), sizeof(((DTC::{name}*)0)->{member}));')
    cpp.append(f'printf("{mt[1]}\t{name}\t__SIZE__\t0\t%zu\\n", sizeof(DTC::{name}));')
    structs.append(dict(name=name, message=mt[1], fields=fields))
cpp.append('}')
(root / 'layout_probe.cpp').write_text('\n'.join(cpp), encoding='utf-8')
subprocess.run(['g++', '-std=c++17', str(root / 'layout_probe.cpp'), '-o', str(root / 'layout_probe.exe')], check=True)
output = subprocess.check_output([str(root / 'layout_probe.exe')], text=True)
(root / 'official-layout.tsv').write_text('message\tstruct\tfield\toffset\tsize\n' + output, encoding='utf-8')
(root / 'inventory.json').write_text(json.dumps(dict(messages=[dict(name=n, id=int(i), deprecated='removed' in c, implementation=implemented.get(int(i))) for n,i,c in constants], structs=structs), indent=2), encoding='utf-8')
print(f'{len(constants)} official message constants, {len(implemented)} implemented IDs, {len(structs)} structures; ABI inventory generated')
