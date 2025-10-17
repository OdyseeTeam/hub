import unittest
from hub.common import expand_query, TooManyClaimSearchParametersError


class TestExpandQuery(unittest.TestCase):

    def test_filter_first_moves_terms_to_filter(self):
        query = expand_query(filter_first=True, channel_ids=['abc123'])
        bool_query = query['query']['bool']
        self.assertEqual(len(bool_query['must']), 0)
        self.assertEqual(len(bool_query['filter']), 1)
        self.assertIn('terms', bool_query['filter'][0])
        self.assertEqual(bool_query['filter'][0]['terms']['channel_id.keyword'], ['abc123'])

    def test_filter_first_false_uses_must(self):
        query = expand_query(filter_first=False, channel_ids=['abc123'])
        bool_query = query['query']['bool']
        self.assertEqual(len(bool_query['filter']), 0)
        self.assertEqual(len(bool_query['must']), 1)
        self.assertIn('terms', bool_query['must'][0])

    def test_text_query_stays_in_must_with_filter_first(self):
        query = expand_query(filter_first=True, text='test search')
        bool_query = query['query']['bool']
        self.assertEqual(len(bool_query['must']), 1)
        self.assertIn('simple_query_string', bool_query['must'][0])
        self.assertEqual(bool_query['must'][0]['simple_query_string']['query'], 'test search')

    def test_leading_wildcard_rejected(self):
        with self.assertRaises(ValueError) as context:
            expand_query(text='*test')
        self.assertIn('Leading wildcards are not allowed', str(context.exception))

    def test_leading_question_mark_rejected(self):
        with self.assertRaises(ValueError) as context:
            expand_query(text='?test')
        self.assertIn('Leading wildcards are not allowed', str(context.exception))

    def test_max_terms_per_clause_enforced(self):
        too_many = ['id' + str(i) for i in range(2049)]
        with self.assertRaises(TooManyClaimSearchParametersError):
            expand_query(max_terms_per_clause=2048, claim_ids=too_many)

    def test_max_terms_per_clause_custom_limit(self):
        many_ids = ['id' + str(i) for i in range(100)]
        with self.assertRaises(TooManyClaimSearchParametersError):
            expand_query(max_terms_per_clause=50, claim_ids=many_ids)

    def test_max_terms_per_clause_under_limit(self):
        many_ids = ['id' + str(i) for i in range(50)]
        query = expand_query(max_terms_per_clause=100, claim_ids=many_ids)
        self.assertIsNotNone(query)

    def test_filter_first_with_claim_type(self):
        query = expand_query(filter_first=True, claim_type='stream')
        bool_query = query['query']['bool']
        self.assertTrue(any('term' in clause for clause in bool_query['filter']))

    def test_filter_first_with_range_query(self):
        query = expand_query(filter_first=True, height='>100')
        bool_query = query['query']['bool']
        self.assertTrue(any('range' in clause for clause in bool_query['filter']))

    def test_filter_first_with_tags(self):
        query = expand_query(filter_first=True, any_tags=['crypto', 'blockchain'])
        bool_query = query['query']['bool']
        self.assertTrue(any('terms' in clause and 'tags.keyword' in clause['terms']
                           for clause in bool_query['filter']))

    def test_signature_valid_uses_bool_should_clause(self):
        query = expand_query(filter_first=True, signature_valid=True)
        bool_query = query['query']['bool']
        has_should = any(
            'bool' in clause and 'should' in clause['bool']
            for clause in bool_query['filter']
        )
        self.assertTrue(has_should)

    def test_release_time_uses_bool_should_clause(self):
        query = expand_query(filter_first=True, release_time=['>100', '<200'])
        bool_query = query['query']['bool']
        has_should = any(
            'bool' in clause and 'should' in clause['bool']
            for clause in bool_query['filter']
        )
        self.assertTrue(has_should)

    def test_has_source_preserves_bool_logic_with_filter_first(self):
        query = expand_query(filter_first=True, has_source=True)
        bool_query = query['query']['bool']
        has_bool_should = any('bool' in clause and 'should' in clause['bool'] for clause in bool_query['filter'])
        self.assertTrue(has_bool_should)

    def test_has_source_complex_without_filter_first(self):
        query = expand_query(filter_first=False, has_source=True)
        bool_query = query['query']['bool']
        has_complex_bool = any('bool' in clause and 'should' in clause['bool']
                               for clause in bool_query['must'])
        self.assertTrue(has_complex_bool)


if __name__ == '__main__':
    unittest.main()
