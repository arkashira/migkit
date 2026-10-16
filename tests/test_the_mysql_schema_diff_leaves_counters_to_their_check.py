"""A table's AUTO_INCREMENT is the counter check's, not the schema
comparison's: the comparison reads cached table statistics, so after the
repair raised a counter it still read the old one and the proof ended
'w9c is different after its repair' (W16N-DEFECTS on MySQL)."""
from migkit.engines.mysql import _without_counters


def test_a_counter_only_statement_is_dropped_with_its_comment():
    text = ('-- Modify "seq_behind" table\n'
            "ALTER TABLE `seq_behind` AUTO_INCREMENT 5000;")
    assert _without_counters(text) == ""


def test_a_counter_beside_a_real_change_keeps_the_change():
    text = ('-- Modify "o" table\n'
            "ALTER TABLE `o` ADD COLUMN `w` int NULL, AUTO_INCREMENT 7;\n"
            '-- Modify "seq_behind" table\n'
            "ALTER TABLE `seq_behind` AUTO_INCREMENT = 5000;\n")
    assert _without_counters(text) == ('-- Modify "o" table\n'
                                       "ALTER TABLE `o` ADD COLUMN `w` int"
                                       " NULL;")
