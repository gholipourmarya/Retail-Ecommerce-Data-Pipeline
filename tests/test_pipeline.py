import csv
import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.pipeline import transform, run

class PipelineTests(unittest.TestCase):
    def test_valid_row(self):
        path = Path(__file__).resolve().parents[1] / 'data' / 'input.csv'
        with path.open(newline='', encoding='utf-8') as handle:
            row = next(csv.DictReader(handle))
        self.assertIn('value', transform(row))
        self.assertIn('flag', transform(row))

    def test_pipeline_quarantines_invalid_rows_and_replaces_previous_output(self):
        with TemporaryDirectory() as directory:
            base = Path(directory)
            (base / 'data').mkdir()
            input_path = base / 'data' / 'input.csv'
            output = base / 'output'
            header = 'product_id,quantity,unit_price,stock\n'
            invalid_rows = (
                'P1,1,NaN,40\n'
                'P1,1,Infinity,40\n'
                'P1,2,1e308,40\n'
                ',1,2.50,40\n'
                'P1,1,2.50,40,extra\n'
                'P1,1,2.50\n'
            )
            with patch('src.pipeline.BASE', base):
                input_path.write_text(header + 'P1,2,2.50,12\n' + invalid_rows, encoding='utf-8')
                self.assertEqual(run(), [{'group': 'P1', 'records': 1, 'total_value': 5.0, 'flags': 1}])

                input_path.write_text(header + 'P1,1,1e308,40\nP1,1,1e308,40\n', encoding='utf-8')
                with self.assertRaisesRegex(ValueError, 'Total value is too large'):
                    run()
                with closing(sqlite3.connect(output / 'warehouse.db')) as con:
                    self.assertEqual(con.execute('SELECT value FROM events').fetchall(), [(5.0,)])

                input_path.write_text(header + invalid_rows, encoding='utf-8')
                self.assertEqual(run(), [])

            self.assertEqual(len(json.loads((output / 'rejected.json').read_text(encoding='utf-8'))), 6)
            self.assertEqual((output / 'clean.jsonl').read_text(encoding='utf-8'), '')
            self.assertEqual(json.loads((output / 'summary.json').read_text(encoding='utf-8')), [])
            with closing(sqlite3.connect(output / 'warehouse.db')) as con:
                self.assertEqual(con.execute('SELECT COUNT(*) FROM events').fetchone(), (0,))

if __name__ == '__main__': unittest.main()
