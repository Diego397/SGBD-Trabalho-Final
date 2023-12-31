import networkx as nx
import matplotlib.pyplot as plt

class Graph:
	def __init__(self):
		self.adj_list = {}  # Lista de adjacência para armazenar as arestas do grafo
		self.visited = {}  # Dicionário para rastrear os nós visitados durante a busca

	def add_edge(self, node1, node2):
		# Verifica se a aresta já existe antes de adicioná-la
		if node1 in self.adj_list and node2 in self.adj_list[node1]:
			return  # A aresta já existe, não é necessário adicioná-la novamente

		# Adiciona a aresta ao nó
		self.adj_list.setdefault(node1, []).append(node2)

	def remove_node(self, node):
		if node in self.adj_list:
			del self.adj_list[node]

			# Remove o nó de todos os nós adjacentes
			self.adj_list = {node1: [node2 for node2 in nodes if node2 != node] for node1, nodes in self.adj_list.items()}

		return self

	def has_cycle(self):
		self.visited = {node: False for node in self.adj_list}

		# Percorre todos os nós do grafo e chama a busca em profundidade (DFS) para verificar ciclos
		for node in self.adj_list:
			if not self.visited[node]:
				cycle_nodes = self.dfs_has_cycle(node, [])
				if cycle_nodes:
					return cycle_nodes  # Retorna a lista de nós que compõem o ciclo

		return None  # Não há ciclo no grafo

	def dfs_has_cycle(self, node, path):
		self.visited[node] = True
		path.append(node)

		for neighbor in self.adj_list[node]:
			if neighbor not in self.visited:
				continue

			if neighbor in path:
				# Encontrou um ciclo
				cycle_nodes = path[path.index(neighbor):] + [neighbor]
				return cycle_nodes

			# Chama a busca em profundidade para o vizinho
			cycle_nodes = self.dfs_has_cycle(neighbor, path)
			if cycle_nodes:
				return cycle_nodes

		path.pop()
		return None

	def print_graph(self):
		for node, adj_nodes in self.adj_list.items():
			print(node, "->", adj_nodes)

# Plotar o grafo
def plot_graph(graph):
	G = nx.DiGraph(graph.adj_list)
	pos = nx.spring_layout(G)

	nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=500, font_size=12, font_weight="bold", edge_color="gray", arrows=True)

	plt.title("Graph Representation")
	plt.show()

# Pegar a transação mais recente
def last_transaction(transaction_id_list, operations_table):
	aux_list = []
	for operation in operations_table:
		if operation[1] in transaction_id_list and operation[1] not in aux_list:
			aux_list.append(operation[1])

	return aux_list[-1]  # Retorna o ID da transação mais recente

def update_operation_status(operations_table, transaction_id):
	updated_table = []
	for operation in operations_table:
		if operation[1] == transaction_id:
			operation[4] = "ABORTED"

		updated_table.append(operation)

	return updated_table

def detect_deadlock(wait_table, wait_graph):
	cycle_nodes = wait_graph.has_cycle()
	if cycle_nodes:
		return cycle_nodes  # Retorna a lista de nós que compõem o ciclo

	return None  # Não há deadlock

# Parte da multigranulosidade:
def process_operation(line, operations_table, wait_table, wait_graph, aborted_transactions_list, lsn):
	if (line[0] != 'i'):
		operation_type = line[0] + "l"
		transaction_id = "T" + line[1]

		if transaction_id in aborted_transactions_list:
			print(lsn, transaction_id, operation_type, "ABORTED")
			obj = line[3]
			operations_table.append([lsn, transaction_id, obj, operation_type, "ABORTED"])
			return lsn

		if operation_type == "rl":
			status = 1
			obj = line[3]

			# Verifica se há conflitos com outras operações e adiciona à lista de espera, se necessário
			for operation in operations_table:
				if (
					operation[1] != transaction_id
					and operation[2] == obj
					and (operation[3] == "cl" or operation[3] == "icl")
					and operation[4] != "ABORTED"
				):
					status = 3
					wait_table.append([lsn, transaction_id, operation[0], operation[1]])
					wait_graph.add_edge(transaction_id, operation[1])

			operations_table.append([lsn, transaction_id, obj, operation_type, status])
			lsn += 1

		if operation_type == "wl":
			status = 1
			obj = line[3]

			# Verifica se há conflitos com outras operações e adiciona à lista de espera, se necessário
			for operation in operations_table:
				if (
					operation[1] != transaction_id
					and operation[2] == obj
					and (operation[3] == "wl" or operation[3] == "cl" or operation[3] == "iwl" or operation[3] == "icl")
					and operation[4] != "ABORTED"
				):
					status = 3
					wait_table.append([lsn, transaction_id, operation[0], operation[1]])
					wait_graph.add_edge(transaction_id, operation[1])

			operations_table.append([lsn, transaction_id, obj, operation_type, status])
			lsn += 1

		if operation_type == "cl":
			for write_operation in operations_table:
				status = 1
				if (
					write_operation[1] == transaction_id
					and write_operation[3] == "wl"
				):
					obj = write_operation[2]
					for operation in operations_table:
						if (
							operation[1] != transaction_id
							and operation[2] == obj
							and (operation[3] == "wl" or operation[3] == "cl" or operation[3] == "rl" or operation[3] == "iwl" or operation[3] == "icl" or operation[3] == "irl")
							and operation[4] != "ABORTED"
						):
							status = 3
							wait_table.append([lsn, transaction_id, operation[0], operation[1]])
							wait_graph.add_edge(transaction_id, operation[1])

					operations_table.append([lsn, transaction_id, write_operation[2], operation_type, status])
					lsn += 1
	else:
		operation_type = line[0] + line[1] + 'l'
		transaction_id = "T" + line[2]

		if transaction_id in aborted_transactions_list:
			print(lsn, transaction_id, operation_type, "ABORTED")
			obj = line[4]
			operations_table.append([lsn, transaction_id, obj, operation_type, "ABORTED"])
			return lsn

		if operation_type == "irl":
			status = 1
			obj = line[4]
			for operation in operations_table:
				if (
					operation[1] != transaction_id
					and operation[2] == obj
					and (operation[3] == "cl")
					and operation[4] != "ABORTED"
				):
					status = 3
					wait_table.append([lsn, transaction_id, operation[0], operation[1]])
					wait_graph.add_edge(transaction_id, operation[1])

			operations_table.append([lsn, transaction_id, obj, operation_type, status])
			lsn += 1

		if operation_type == "iwl":
			status = 1
			obj = line[4]
			for operation in operations_table:
				if (
					operation[1] != transaction_id
					and operation[2] == obj
					and (operation[3] == "wl" or operation[3] == "cl")
					and operation[4] != "ABORTED"
				):
					status = 3
					wait_table.append([lsn, transaction_id, operation[0], operation[1]])
					wait_graph.add_edge(transaction_id, operation[1])

			operations_table.append([lsn, transaction_id, obj, operation_type, status])
			lsn += 1

		if operation_type == "icl":
			for write_operation in operations_table:
				status = 1
				if (
					write_operation[1] == transaction_id
					and write_operation[3] == "wl"
				):
					obj = write_operation[2]
					for operation in operations_table:
						if (
							operation[1] != transaction_id
							and operation[2] == obj
							and (operation[3] == "wl" or operation[3] == "cl" or operation[3] == "rl")
							and operation[4] != "ABORTED"
						):
							status = 3
							wait_table.append([lsn, transaction_id, operation[0], operation[1]])
							wait_graph.add_edge(transaction_id, operation[1])

					operations_table.append([lsn, transaction_id, write_operation[2], operation_type, status])
					lsn += 1
	return lsn

def write_tables(operations_table, wait_table, aborted_transactions_list):
	print("LSN	TRID	OBID	TIR STATUS")
	with open("out.txt", "w") as arq_write:
		arq_write.write("LSN	TRID	OBID	TIR STATUS\n")
		for operation in operations_table:
			line = f"{operation[0]}	{operation[1]}	{operation[2]}	{operation[3]}	{operation[4]}\n"
			arq_write.write(line)
			print(line, end="")

	print("\nTransações Abortadas:")
	with open("out.txt", "a") as arq_write:
		arq_write.write("\nTransações Abortadas:\n")
		for transaction_id in aborted_transactions_list:
			arq_write.write(f"{transaction_id}\n")
			print(transaction_id)

def main():
	operations_table = []
	wait_table = []
	wait_graph = Graph()
	aborted_transactions_list = []
	lsn = 1
	deadlock_check_interval = 1  # Verificar deadlock a cada 1 operação

	with open("in.txt") as arq_read:
		for line in arq_read:
			line = line.strip()
			lsn = process_operation(line, operations_table, wait_table, wait_graph, aborted_transactions_list, lsn)

			# Verificar deadlock a cada `deadlock_check_interval` operações
			if lsn % deadlock_check_interval == 0:
				deadlock_nodes = detect_deadlock(wait_table, wait_graph)
				if deadlock_nodes:
					print("Deadlock detectado!")
					print("Transações em deadlock:")
					for node in deadlock_nodes:
						print(node, end=" ")
					print("\n\nAbortando transação mais recente em deadlock.")
					last_transaction_id = last_transaction(deadlock_nodes, operations_table)
					aborted_transactions_list.append(last_transaction_id)
					operations_table = update_operation_status(operations_table, last_transaction_id)
					# wait_graph.print_graph()
					wait_graph = wait_graph.remove_node(last_transaction_id)
					# wait_graph.print_graph()

	write_tables(operations_table, wait_table, aborted_transactions_list)
	# wait_graph.print_graph()
	plot_graph(wait_graph)

if __name__ == "__main__":
	main()