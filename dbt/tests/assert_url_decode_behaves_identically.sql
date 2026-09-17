/*
    url_decode is the one function this project implements itself for a warehouse
    (Postgres; see macros/cross_warehouse.sql). This pins its behaviour to the same
    expected output on EVERY target, so the DuckDB built-in and the Postgres function
    cannot drift apart unnoticed.

    Each case was chosen because a plausible implementation gets it wrong: `+` is not a
    space, a malformed or truncated escape is kept literally, and bytes that do not form
    valid UTF-8 leave the input unchanged instead of failing the build - which DuckDB's
    raw url_decode did not do.
*/

with cases (input, expected) as (

    values
        ('Apple_Inc.',   'Apple_Inc.'),
        ('Caf%C3%A9',    'Café'),
        ('Tom%27s',      'Tom''s'),
        ('%E3%82%A2',    'ア'),
        ('%F0%9F%8D%8E', '🍎'),
        ('%41%42',       'AB'),
        ('a+b%20c',      'a+b c'),
        ('50%25_off',    '50%_off'),
        ('%%41',         '%A'),
        ('100%',         '100%'),
        ('%zz',          '%zz'),
        ('%4',           '%4'),
        ('%E3',          '%E3'),
        ('%E3x',         '%E3x')

)

select input, expected, {{ url_decode('input') }} as actual
from cases
where {{ url_decode('input') }} is distinct from expected
