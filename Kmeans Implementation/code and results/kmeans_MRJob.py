# KMeans MapReduce for 'n' points of 'k' dimensions
from mrjob.job import MRJob
from mrjob.step import MRStep
import sys
import datetime

# class to define k-clusters
class Cluster:
	# defining the constructor for the class
	def __init__(self, clusterID, centroid_coord):	
		self.total_coord = centroid_coord	# total_coord: list to store sum corr. to 'k' dimensions for points assigned to cluster	
		self.centroid_coord = centroid_coord	# centroid_coord: list of k-coordinates of the initial centroid point
		self.count = 1				# count: stores the number of points assigned to the cluster
		self.clusterID = clusterID		# clusterID: to assign points to corresponding clusters

# class to define protocols to read the input file and convert the input into the format to be fed to the mapper
class kmeansProtocol(object):
	'''Reads data line by line and converts into key-value pairs to be given to the mapper
	key: 
		cluster id
	value:
		list of coordinates of the entities to be clustered
	'''
	def read(self, line):
		list_coord = []
		splitArray = line.split("\t")
		cluster_id = int(splitArray[0])	# first element read is the cluster id
		# rest all values appended in list as list of coordinates of a given point
		for i in range(1,len(splitArray)-1):
			list_coord.append(float(splitArray[i]));
		list_coord.append(float(splitArray[len(splitArray)-1].split("\n")[0]))
		return (cluster_id,list_coord)
	
	def write(self, key, value):
		string = str(cluster_id) 
		for i in range(list_coord):
			string = string + "\t" + list_coord[i]
		return string

# class to implement kmeans clustering inheriting MRJob as the base class
class kmeansClustering(MRJob):
	#read the input in the kmeans format
	INPUT_PROTOCOL = kmeansProtocol
	
	#overriding mrjob functions to implement kmeans
	def configure_options(self):
		super(kmeansClustering,self).configure_options()
		
		self.add_passthrough_option( '--iterations', dest='iterations', default=50, type='int', help='number of iterations to run')
		self.centroids = {}
		
		f = open('1_centroids.txt', 'r')
		while True:
			pattern = f.readline()
			if not pattern:
				break
			list_coord = []
			splitArray = pattern.split("\t")
			clusterID = int(splitArray[0])
			for i in range(1,len(splitArray)-1):
				list_coord.append(float(splitArray[i]));
			list_coord.append(float(splitArray[len(splitArray)-1].split("\n")[0]))
			self.centroids[int(clusterID)] = Cluster(int(clusterID), list_coord)
		f.close()
	
	# assigns points to the clusters, updates cluster ids, calculates partial sum and count corresponding to each cluster
	def Mapper(self, key, value):
		clusterID = key
		coords  = value
		minDistance = sys.maxint
		'''centroidDict maintains the data corresponding to each cluster
		key: cluster id
		value: list of lists corresponding to each id
			first element would be the list of partial sums for each dimension while second element being the partial count of
			number of elements in the cluster'''
		centroidDict = {}	
		
		for cluster in self.centroids.values():
			dist = 0
			for i in range(len(coords)):
				if cluster.count!=0:
					dist = dist + pow((cluster.total_coord[i]/cluster.count - coords[i]),2)
			if dist < minDistance:
				minDistance = dist		#updating minimum distance
				clusterID = cluster.clusterID	#updating cluster id
				
		# CASE 1: if the given point is already in the cluster or is the centroid of cluster
		if self.centroids[clusterID] == coords or key==clusterID:
			yield clusterID, ("point",coords,0)	#input for the mapper for next iteration
				
		else:
			#CASE 2: if the point was previously in some other cluster and now the cluster id changes
			if key != clusterID and key!=0:
				clusterList = centroidDict[key]
				for i in range(len(coords)):
					clusterList[0][i] = clusterList[0][i] - coords[i]
				clusterList[1] = clusterList[1] - 1

			#CASE 3: if the given point has not yet been assigned to the new cluster
			if clusterID not in centroidDict.keys():
				temp_list = []
				temp_list.append(coords)
				temp_list.append(1)
				centroidDict[clusterID] = temp_list
			else:
				clusterList = centroidDict[clusterID]
				for i in range(len(coords)):
					clusterList[0][i] = clusterList[0][i] + coords[i]
				clusterList[1] = clusterList[1] + 1
			yield clusterID, ("point",coords,0)
			
		for result in centroidDict.values():
			yield clusterID, ("accumulated_score",result[0], result[1]);	#input for the reducer
						
	# recalculates and updates the centroids of the clusters
	def Reducer(self, key, values):
		for (pattern, sum_coords, count) in values:
			if pattern == "point":
				yield key, sum_coords	# input for the mapper for the next iteration
			elif pattern == "accumulated_score":
				# computing total sum and count corresponding to each cluster
				for i in range(len(self.centroids[key].total_coord)):
					self.centroids[key].total_coord[i] = self.centroids[key].total_coord[i] + sum_coords[i]
				self.centroids[key].count = self.centroids[key].count + count
				
	
	def steps(self):
		return [MRStep(mapper=self.Mapper, reducer=self.Reducer)]*self.options.iterations

# calling kmeansClustering function to run kmeans mapreduce
if __name__ == "__main__":
	start = datetime.datetime.now()
	kmeansClustering.run()
	stop = datetime.datetime.now()
	print "time taken: " , stop-start
