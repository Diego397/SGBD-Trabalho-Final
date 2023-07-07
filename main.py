arq_read = open("in.txt")
table = []
wait_table = []
lsn = 1

for linha in arq_read:
	operation_read = linha[0] + 'l'
	transaction_id_read = 'T' + linha[1]

	match operation_read:
		case 'rl':    
			obj_read = linha[3]
			table.append([lsn, transaction_id_read, obj_read, operation_read, 1])
			lsn = lsn + 1
		
		case 'wl':
			status = 1
			obj_read = linha[3]
			for i in range(len(table)):
				if (table[i][1] != transaction_id_read and table[i][2] == obj_read and (table[i][3] == "wl" or table[i][3] == "cl")):
					wait_table.append([lsn, transaction_id_read, table[i][0], table[i][1]])
					status = 3

			table.append([lsn, transaction_id_read, obj_read, operation_read, status])
			lsn = lsn + 1
		
		case 'cl':
			for i in range(len(table)):
				status = 1
				if (table[i][1] == transaction_id_read and table[i][3] == "wl"):
					obj = table[i][2]
					for j in range(len(table)):
						if (table[j][1] != transaction_id_read and table[j][2] == obj and (table[j][3] == "wl" or table[j][3] == "cl" or table[j][3] == "rl")):
							status = 3
							wait_table.append([lsn, transaction_id_read, table[j][0], table[j][1]])
					table.append([lsn, transaction_id_read, table[i][2], operation_read, status])
					lsn = lsn + 1    


print("LSN	TRID	OBID	TIR	STATUS")
for i in range(len(table)):
	print(f"{table[i][0]}	{table[i][1]}	{table[i][2]}	{table[i][3]}	{table[i][4]}")

print("\nTabela de esperas")
for i in range(len(wait_table)):
	print(f"[{wait_table[i][0]}] {wait_table[i][1]} espera [{wait_table[i][2]}] {wait_table[i][3]}")