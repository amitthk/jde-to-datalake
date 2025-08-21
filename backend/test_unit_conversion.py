#!/usr/bin/env python3
# Test unit conversion

unit_map = {
    'KG': 'kg',
    'EA': 'each',
    'LT': 'L',
    'M2': 'm2',
    'C2': 'c2',
    'PK': 'pack',
    'ST': 'ST',
    'FN': 'FN',
    'GR': 'g',
    'ML': 'mL'
}

reverse_unit_map = {v: k for k, v in unit_map.items()}

def convert_unit(unit, direction='from_jde'):
    """Convert a unit between Data lake and JDE formats."""
    if direction == 'from_jde':
        return unit_map.get(unit.upper(), unit.lower())
    elif direction == 'to_jde':
        # Try both original case and lowercase to handle mixed case reverse_unit_map
        return reverse_unit_map.get(unit, reverse_unit_map.get(unit.lower(), unit.upper()))

print('reverse_unit_map:', reverse_unit_map)
print('Testing L (uppercase) to JDE:', convert_unit('L', 'to_jde'))
print('Testing l (lowercase) to JDE:', convert_unit('l', 'to_jde'))
print('Testing kg to JDE:', convert_unit('kg', 'to_jde'))
print('Testing KG to JDE:', convert_unit('KG', 'to_jde'))
print('Testing ST to JDE:', convert_unit('ST', 'to_jde'))
print('Testing g to JDE:', convert_unit('g', 'to_jde'))
print('Testing mL to JDE:', convert_unit('mL', 'to_jde'))
