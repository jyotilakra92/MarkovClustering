from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime

class MatrixValueProtocol(object):
    """Read in a line as ``(None, line)``. Write out ``(key, value)``
    as ``value``. 
	key - (i,j)
	value - float value
    """
    def read(self, line):
		row, col, value = line.split("\t")
		return ((int(row), int(col)), float(value))

    def write(self, key, value):
        return "%d\t%d\t%f" %(row, col, value)
        
class MRMarkovClustering(MRJob):
	# read the input in matrix value format
	INPUT_PROTOCOL = MatrixValueProtocol
	# OUTPUT_PROTOCOL = MatrixValueProtocol
	
	SORT_VALUES = True

	JOBCONF = {	'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
				'mapred.text.key.comparator.options': '-k1n' ,}
	
	def configure_options(self):
		super(MRMarkovClustering, self).configure_options()
		
		self.add_passthrough_option(
			'--iterations', dest='iterations', default=10, type='int',
			help='number of iterations to run')
		
		self.add_passthrough_option(
			'--matrix-size', dest='matrix_size', default=12, type='int',
			help='size of the matrix')
			
		self.add_passthrough_option(
			'--inflation-parameter', dest='inflation_parameter', default=2, type='int',
			help='power of the each value taken to do inflation')
            
	def normalizeMapper(self, row_col, value):
		"""
		Mapper : adds "matVal" identifier in key-value pair
		
		Input: "(row, col), value"
		
		Ouput: "col, (row, 'matVal', value)"
		
		"""
		row, col = row_col
		yield col, (row, 'matVal', value)

	def normalizeCombiner(self, col, colValues):
		"""
		Combiner: Adds all the column values for each column and emits it
		"""
		sumColValues = 0
		
		for (row, valueType, value) in list(colValues):
				yield col, (row, 'matVal', value)
				sumColValues += value
		
		# row and column indices start with 1, sum value will be emitted with 
		# row 0 to get it at top in sorted order.
		yield col, (0, 'sumCol', sumColValues)
		
	def normalizeReducer(self, col, colValues):
		"""
		Reducer : Emits all the normalized values
		
		Input: "col, (valueId, row, value)"
		
		Output: "row, col, value/sum of all the values of its column"
		
		"""
		
		sumColValues = 0
		for (row, valueType, value) in colValues:
			if valueType == 'sumCol':
				sumColValues += value
			elif valueType == 'matVal' and sumColValues != 0 and value/sumColValues > 0.065:
				yield (row, col), value/sumColValues
				
	def expandMapper(self, row_col, value):
		"""
		Mapper : emits this key value pair as many times as it is required in the
				calculation of the result matrix
		
		Input: "(row, col), value"
		
		Ouput: "(row, k), (col, value)" or "(k, col), (row, value)"
		
		"""
		row, col = row_col
		for k in range(self.options.matrix_size):		
			yield (row, k+1), (col, value)
			yield (k+1, col), (row, value)
		
	def expandReducer(self, res_matrix_id, listValues):
		"""
		Reducer : Multiplies and sums values for each (i, k) of the result matrix
		
		Input: "(row, k), listValues" or "(k, col), listValues"
			listValues: list of tuples of the form ( row_or_col, value)
		
		Output: "(row, col), value"
		
		"""
		row_or_k, k_or_col = res_matrix_id
		
		aggrDict = {}
		
		for value in  listValues :
			a = 1
			if value[0] in aggrDict.keys():
				a = aggrDict[value[0]]
				
			aggrDict[value[0]] = a * value[1]
			
		res = 0
		for index in  aggrDict.keys():
			res += aggrDict[index]
			
		yield (row_or_k, k_or_col), res
		
	def inflateMapper(self, row_col, value):
		"""
		Mapper : emits inflated value by taking power of each value
		
		Input: "(row, col), value"
		
		Ouput: "(row, col), value^r"
		
		"""
		row, col = row_col
		yield (row, col), pow(value, self.options.inflation_parameter)
		
	
	def steps(self):
		stepList = [MRStep(mapper=self.normalizeMapper,
						combiner=self.normalizeCombiner,
						reducer=self.normalizeReducer),
					MRStep(mapper=self.expandMapper,
						reducer=self.expandReducer),
					MRStep(mapper=self.inflateMapper)] * self.options.iterations
		stepList.append(MRStep(mapper=self.normalizeMapper,
						combiner=self.normalizeCombiner,
						reducer=self.normalizeReducer))
		
		return stepList
		
					
if __name__ == '__main__':
	start = datetime.datetime.now()
	MRMarkovClustering.run()
	end = datetime.datetime.now()
	print ("Time taken : " + str(end - start))
