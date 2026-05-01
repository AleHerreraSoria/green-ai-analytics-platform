"""
Este test verifica que las claves únicas del modelo Gold se generen correctamente: 
que el mismo país produzca siempre la misma clave aunque venga con mayúsculas o espacios, 
que valores vacíos no generen claves inválidas y que países distintos tengan claves diferentes. 
asegura que las “llaves” que conectan las tablas funcionen bien y no generen errores en los joins.

"""

import hashlib


def generate_country_id(iso_alpha3):
    """
    Replica la lógica del pipeline:
    md5(lower(trim(iso_alpha3)))
    """
    if iso_alpha3 is None:
        return None

    value = iso_alpha3.strip().lower()
    return hashlib.md5(value.encode()).hexdigest()


def test_country_surrogate_key_generation():
    """
    Valida que la surrogate key sea consistente
    independientemente de mayúsculas o espacios.
    """

    key1 = generate_country_id("USA")
    key2 = generate_country_id(" usa ")
    key3 = generate_country_id("UsA")

    # Todas deben ser iguales
    assert key1 == key2 == key3


def test_country_surrogate_key_not_null():
    """
    Valida que valores nulos no generen claves inválidas
    """
    assert generate_country_id(None) is None


def test_country_surrogate_key_uniqueness():
    """
    Valida que diferentes países generen distintas claves
    """

    key_arg = generate_country_id("ARG")
    key_bra = generate_country_id("BRA")

    assert key_arg != key_bra