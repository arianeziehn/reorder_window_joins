import unittest

from JoinOptimizerMockUp import JoinOptimizer


class TestJoinOptimizer(unittest.TestCase):

    def setUp(self):
        self.stream_names = ['x', 'y', 'z']
        self.stream_freq = {
            'x': 1000,
            'y': 2000,
            'z': 500
        }
        self.window_lengths = {
            ('x', 'y'): 16,
            ('xy', 'z'): 4,
        }

    # Q7
    def test_PT_with_w_1_gthan_w_2(self):
        self.stream_freq = {
            'x': 50,
            'y': 50,
            'z': 50
        }

        self.window_lengths = {
            ('x', 'y'): 16,
            ('xy', 'z'): 4,
        }

        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            window_lengths=self.window_lengths
        )

        expected_window_lengths = {
            ('x', 'y'): 16,
            ('xy', 'z'): 16,
            ('x', 'z'): 16,
            ('y', 'x'): 16,
            ('y', 'z'): 16,
            ('z', 'x'): 16,
            ('z', 'y'): 16,
        }

        self.assertDictEqual(expected_window_lengths, optimizer.generated_window_lengths)
        self.assertDictEqual(optimizer.window_lengths, self.window_lengths)
        join_plans_with_costs = optimizer.optimize()

        self.assertTrue(len(join_plans_with_costs) == 6)

        i = 1
        for plan, cost in join_plans_with_costs:
            # print(f"({i}, {cost:.2f})")
            if i == 1 or i == 2:
                self.assertTrue(cost == 1344000.0)
            else:
                self.assertTrue(cost == 10304000.0)
            i += 1

    # Q7
    def test_PT_with_w_1_lthan_w_2(self):
        self.stream_freq = {
            'x': 50,
            'y': 50,
            'z': 50
        }

        self.window_lengths = {
            ('x', 'y'): 4,
            ('xy', 'z'): 8,
        }

        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            window_lengths=self.window_lengths
        )

        expected_window_lengths = {
            ('x', 'y'): 8,
            ('xy', 'z'): 8,
            ('x', 'z'): 8,
            ('y', 'x'): 8,
            ('y', 'z'): 8,
            ('z', 'x'): 8,
            ('z', 'y'): 8,
        }

        self.assertDictEqual(expected_window_lengths, optimizer.generated_window_lengths)
        self.assertDictEqual(optimizer.window_lengths, self.window_lengths)
        join_plans_with_costs = optimizer.optimize()

        self.assertTrue(len(join_plans_with_costs) == 6)

        i = 1
        for plan, cost in join_plans_with_costs:
            # print(f"({i}, {cost:.2f})")
            if i == 1 or i == 2:
                self.assertTrue(cost == 324000.0)
            else:
                self.assertTrue(cost == 1296000.0)
            i += 1

    # Q7
    def test_ET_with_w_1_eq_w_2(self):
        self.stream_freq = {
            'x': 50,
            'y': 50,
            'z': 50
        }

        self.window_lengths = {
            ('x', 'y'): 8,
            ('xy', 'z'): 8,
        }

        propagation_time = 'x'  # Propagation time for the join x,y

        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            time_domain='Et',
            window_slide=16,
            propagation_time=propagation_time,
            window_lengths=self.window_lengths
        )

        expected_window_lengths = {
            ('x', 'y'): 8,
            ('xy', 'z'): 8,
            ('x', 'z'): 8,
            ('y', 'x'): 8,
            ('y', 'z'): 8,
            ('z', 'x'): 8,
            ('z', 'y'): 8,
        }

        self.assertDictEqual(expected_window_lengths, optimizer.generated_window_lengths)
        self.assertDictEqual(optimizer.window_lengths, self.window_lengths)
        join_plans_with_costs = optimizer.optimize()
        self.assertTrue(len(join_plans_with_costs) == 6)

        i = 1
        for plan, cost in join_plans_with_costs:
            print(f"({i}, {cost:.2f})")
            self.assertTrue(cost == 656000.0)
            i += 1

    # Q7
    def test_ET_with_w_1_gthan_w_2(self):
        self.stream_freq = {
            'x': 50,
            'y': 50,
            'z': 50
        }

        self.window_lengths = {
            ('x', 'y'): 4,
            ('xy', 'z'): 8,
        }

        propagation_time = 'x'  # Propagation time for the join x,y

        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            time_domain='Et',
            window_slide=16,
            propagation_time=propagation_time,
            window_lengths=self.window_lengths
        )

        expected_window_lengths = {
            ('x', 'y'): 4,  # given
            ('xy', 'z'): 8,  # given
            ('x', 'z'): 8,  # due to propagation of x as timestamp
            ('y', 'x'): 4,  # given (vs)
            ('y', 'z'): 8,  # largest window
            ('z', 'x'): 8,
            ('z', 'y'): 8,
        }

        self.assertDictEqual(expected_window_lengths, optimizer.generated_window_lengths)
        self.assertDictEqual(optimizer.window_lengths, self.window_lengths)
        join_plans_with_costs = optimizer.optimize()
        self.assertTrue(len(join_plans_with_costs) == 6)

        i = 1
        for plan, cost in join_plans_with_costs:
            print(f"({i}, {cost:.2f})")
            if i == 1 or i == 2:
                self.assertTrue(cost == 324000.0)
            else:
                self.assertTrue(cost == 336000.0)
            i += 1

    # Q7
    def test_ET_with_w_1_gthan_w_2(self):
        self.stream_freq = {
            'x': 50,
            'y': 50,
            'z': 50
        }

        self.window_lengths = {
            ('x', 'y'): 8,
            ('xy', 'z'): 4,
        }

        propagation_time = 'x'  # Propagation time for the join x,y

        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            time_domain='Et',
            window_slide=16,
            propagation_time=propagation_time,
            window_lengths=self.window_lengths
        )

        expected_window_lengths = {
            ('x', 'y'): 8,  # given
            ('xy', 'z'): 4,  # given
            ('x', 'z'): 4,  # due to propagation of x as timestamp
            ('y', 'x'): 8,  # given (vs)
            ('y', 'z'): 8,  # largest window
            ('z', 'x'): 4,
            ('z', 'y'): 8,
        }

        self.assertDictEqual(expected_window_lengths, optimizer.generated_window_lengths)
        self.assertDictEqual(optimizer.window_lengths, self.window_lengths)
        join_plans_with_costs = optimizer.optimize()
        self.assertTrue(len(join_plans_with_costs) == 6)

        i = 1
        for plan, cost in join_plans_with_costs:
            print(f"({i}, {cost:.2f})")
            if i == 1 or i == 2:
                self.assertTrue(cost == 324000.0)
            else:
                self.assertTrue(cost == 336000.0)
            i += 1

    def test_calculate_cost(self):
        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            window_lengths=self.window_lengths
        )
        plan = ('x', 'y', 'z')
        cost = optimizer.calculate_cost(plan)
        self.assertIsInstance(cost, float)
        self.assertGreater(cost, 0)

    def test_optimize(self):
        optimizer = JoinOptimizer(
            self.stream_names,
            self.stream_freq,
            window_lengths=self.window_lengths
        )
        join_plans_with_costs = optimizer.optimize()
        self.assertIsInstance(join_plans_with_costs, list)
        self.assertEqual(len(join_plans_with_costs), 6)
        self.assertLessEqual(join_plans_with_costs[0][1], join_plans_with_costs[-1][1])


if __name__ == '__main__':
    unittest.main()
