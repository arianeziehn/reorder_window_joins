from itertools import permutations, combinations


class JoinOptimizer:
    def __init__(self, stream_names, stream_freq, window_lengths, join_selectivity=0.1,
                 time_domain='Pt', window_slide=16, window_type='sliding', propagation_time=None):
        """
        Initialize the JoinOptimizer with the list of streams, their event frequencies,
        and additional window join parameters.

        :param stream_names: List of stream names to join.
        :param stream_freq: Dictionary mapping stream names to their event frequencies.
        :param window_lengths: Dictionary mapping tuples of stream pairs to their window lengths.
        :param join_selectivity: Assumed selectivity of the joins (default is 0.1).
        :param time_domain: Time domain for the window join ('Pt' for processing time or 'Et' for event time).
        :param window_slide: Slide of the window in time units.
        :param window_type: Type of window ('sliding', 'session', 'interval').
        """
        self.stream_names = stream_names
        self.stream_freq = stream_freq
        self.join_selectivity = join_selectivity
        self.time_domain = time_domain
        self.window_slide = window_slide
        self.window_type = window_type
        self.window_lengths = window_lengths  # the input window parameters
        self.propagation_time = propagation_time  # Propagation times for event time domain
        self.max_length = max(window_lengths.values())
        self.min_length = min(window_lengths.values())
        self.max_window_size_rr = False
        self.add_filter = False
        self.original_join_plans = []

        # Always generate default window lengths first
        self.generated_window_lengths = window_lengths.copy()
        self._generate_window_lengths()
        if self.time_domain == 'Pt':
            self._adjust_window_lengths_pt()
        elif self.time_domain == 'Et':
            self._adjust_window_lengths_et()
            print(self.window_lengths)
            print(self.generated_window_lengths)

    def _generate_window_lengths(self):
        all_pairs = list(permutations(self.stream_names, 2))
        for pair in all_pairs:
            if pair not in self.generated_window_lengths:
                self.generated_window_lengths.update({pair: float(0)})  # Placeholder

    def _adjust_window_lengths_pt(self):
        for key in self.generated_window_lengths:
            self.generated_window_lengths[key] = self.max_length

    def _adjust_window_lengths_et(self):

        for pair, length in self.generated_window_lengths.items():
            if (pair[0], pair[1]) not in self.window_lengths and self.generated_window_lengths[
                (pair[0], pair[1])] == 0.0:
                self.generated_window_lengths[(pair[0], pair[1])] = self.max_length
            if str(self.propagation_time) in pair[0] and len(pair[0]) >= 2:
                self.generated_window_lengths[(self.propagation_time, pair[1])] = length
                self.generated_window_lengths[(pair[1], self.propagation_time)] = length
            if (pair[0], pair[1]) in self.window_lengths and len(pair[0]) < 2:
                self.generated_window_lengths[(pair[0], pair[1])] = self.window_lengths[(pair[0], pair[1])]
                self.generated_window_lengths[(pair[1], pair[0])] = self.window_lengths[(pair[0], pair[1])]

    def generate_join_plans(self):
        """
        Generate all possible join plans considering the windowing rules for Pt domain.

        :return: A list of join plans.
        """
        fixed_first_stream = self.stream_names[0]
        second_first_stream = self.stream_names[1]
        join_plans = []

        for perm in permutations(self.stream_names[2:]):
            self.original_join_plans.append([fixed_first_stream, second_first_stream] + list(perm))
            self.original_join_plans.append([second_first_stream, fixed_first_stream] + list(perm))

        # Handling sliding window case with Pt
        if self.time_domain == 'Pt' and self.window_type == 'sliding':
            # slide > size
            if self.window_slide > min(self.window_lengths.values()):
                if len(set(self.window_lengths.values())) > 1:
                    self.add_filter = True
                    self.max_window_size_rr = True
                join_plans = list(permutations(self.stream_names))
            # slide < size # for PT no reordering
            else:
                join_plans = self.orig_join_plan
        elif self.time_domain == 'Et' and self.window_type == 'sliding':
            # slide > size
            if self.window_slide > min(self.window_lengths.values()):
                # if len(set(self.window_lengths.values())) > 1:
                #    self.add_filter = True
                #    self.max_window_size_rr = True
                join_plans = list(permutations(self.stream_names))
        return join_plans

    def calculate_cost(self, plan):
        """
        Calculate the cost of a given join plan. This includes factors for windowing joins.

        :param plan: The join plan (list of stream names in the join order).
        :return: Calculated cost (float).
        """
        print('start with aktueller Plan', plan)
        cost = 0
        used_windows = set()
        current_result_size = self.stream_freq[plan[0]]

        for i in range(1, len(plan)):
            prev_stream = plan[i - 1]
            next_stream = plan[i]
            next_stream_size = self.stream_freq[next_stream]

            if (list(plan)) in self.original_join_plans:
                if i > 1:
                    prev_streams_perm = list(permutations(plan[0:i]))
                    prev_streams = [''.join(tup) for tup in prev_streams_perm]
                else:
                    prev_streams = prev_stream

                for prev in prev_streams:
                    if (prev, next_stream) in self.window_lengths:
                        current_window_length = self.window_lengths[(prev, next_stream)]
                        break
                    elif (next_stream, prev) in self.window_lengths:
                        current_window_length = self.window_lengths[(next_stream, prev)]
                        break
            else:
                if self.time_domain == 'Et':  # plan is permutation
                    if i > 1:
                        prev_streams = [''.join(tup) for tup in plan[0:i]]
                    else:
                        prev_streams = prev_stream

                    print('Test', self.propagation_time == next_stream)
                    if self.propagation_time in prev_streams:
                        prev_stream = self.propagation_time

                    elif self.propagation_time == next_stream:
                        w_length = float('inf')
                        for tup in plan[0:i]:
                            print(next_stream, tup)
                            print(self.generated_window_lengths[(next_stream, tup)])
                            print((next_stream, tup) in self.window_lengths)
                            if (tup, next_stream) in self.generated_window_lengths and w_length > \
                                    self.generated_window_lengths[(tup, next_stream)]:
                                print('hihi', w_length)
                                w_length = self.generated_window_lengths[(tup, next_stream)]
                                prev_stream = tup
                            elif (next_stream, tup) in self.generated_window_lengths and w_length > \
                                    self.generated_window_lengths[(next_stream, tup)]:
                                print('hihi', w_length)
                                print(tup)
                                prev_stream = tup
                                w_length = self.generated_window_lengths[(next_stream, tup)]
                                print(w_length)

                if (prev_stream, next_stream) in self.generated_window_lengths:
                    current_window_length = self.generated_window_lengths[(prev_stream, next_stream)]
                elif (next_stream, prev_stream) in self.generated_window_lengths:
                    current_window_length = self.generated_window_lengths[(next_stream, prev_stream)]
                else:
                    self.window_lengths.update({(prev_stream, next_stream): self.max_length})
                    current_window_length = self.max_length

            print('window length for subplan', current_window_length)
            used_windows.add(current_window_length)

            if i == 1:
                join_cost = current_result_size * next_stream_size * pow(current_window_length,
                                                                         2) * self.join_selectivity
            else:
                if current_window_length < previous_window_length:
                    join_cost = current_result_size * next_stream_size * current_window_length * self.join_selectivity
                    print('test here')
                else:
                    join_cost = current_result_size * (
                            current_window_length / previous_window_length) * next_stream_size * current_window_length * self.join_selectivity
                    print('no here')

                print(join_cost)
                print(current_window_length)
                print(previous_window_length)

            # Adjust cost for windowing
            if self.window_type == 'sliding':

                if self.window_slide < min(self.window_lengths.values()):
                    max_length = max([max(v.values()) for k, v in self.window_lengths.items()])
                    old_length = current_window_length

                    # Apply cost adjustment for different window lengths
                    if old_length != max_length:
                        selectivity_adjustment = (old_length ** 2) / (max_length ** 2)
                        join_cost *= selectivity_adjustment

            print(join_cost)
            if len(set(self.window_lengths.values()).difference(used_windows)) != 0 and i > 1:
                # filter result
                print('adjust window')
                current_result_size = join_cost / (self.min_length / self.max_length)
                cost = cost + join_cost * 2

            else:

                # Modify the result size based on selectivity
                current_result_size = join_cost

                # Total cost with windowing
                cost += join_cost

            used_windows.add(current_window_length)
            previous_window_length = current_window_length

        return cost

    def optimize(self):
        """
        Generate all join plans and calculate their costs.

        :return: A sorted list of tuples (join plan, cost).
        """
        plans = self.generate_join_plans()
        print('Pläne', plans)
        costs = [(plan, self.calculate_cost(plan)) for plan in plans]
        costs.sort(key=lambda x: x[1])
        return costs


# Example usage 1:
stream_names = ['x', 'y', 'z']  # Example streams
stream_freq = {
    'x': 10,  # Number of events per time unit in stream x
    'y': 100,  # Number of events per time unit in stream y
    'z': 1  # Number of events per time unit in stream z
}
window_lengths = {
    ('x', 'y'): 1000,  # Window length for the join x,y
    ('xy', 'z'): 100,  # Window length for the join y,z
}

propagation_time = 'x'  # Propagation time for the join x,y

optimizer = JoinOptimizer(
    stream_names,
    stream_freq,
    join_selectivity=0.1,
    time_domain='Et',
    window_slide=10000,
    window_type='sliding',
    window_lengths=window_lengths,
    propagation_time=propagation_time
)

join_plans_with_costs = optimizer.optimize()

i = 1
for plan, cost in join_plans_with_costs:
    print(f"({i}, {cost:.2f})")
    print(f"Join Plan: {' -> '.join(plan)}, Cost: {cost}")
    i += 1
