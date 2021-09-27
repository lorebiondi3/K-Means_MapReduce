# The k-means clustering algorithm in MapReduce

A MapReduce implementation of the well-known K-Means clustering algorithm.

Let 𝑋 = {𝑥1, ... , 𝑥𝑛} be a set of 𝑛 data points, each with dimension 𝑑. The 𝑘-means problem seeks to find a set of 𝑘 points (called means) 𝑀 = {𝜇1,...,𝜇𝑘} which minimizes the sum of the squared distances between each point in the data set and the mean closest to that point. Finding an exact solution to this problem is NP-hard. However, there are a number of heuristic algorithms which yield good approximate solutions. The standard algorithm for solving the 𝑘-means problem uses an iterative process which guarantees a decrease in total error on each step. The algorithm is as follows:

![Schermata 2021-09-27 alle 10 20 36](https://user-images.githubusercontent.com/73020009/134871128-72b472f7-6c75-4bfc-bb11-0371e9031efa.png)

The convergence criterion is typically when the total error stops changing between steps, in which case a local optimum of the objective function has been reached.In principle, the number of iterations required for the algorithm to fully converge can be very large, but on real datasets the algorithm typically converges in at most a few dozen iterations.

## MapReduce pseudo-code

In the map phase, for each point in the dataset we compute the distance between that point and the k current centroids. Then, we choose the closest centroid and we emit its index along with the point we are considering.

![Schermata 2021-09-27 alle 11 25 04](https://user-images.githubusercontent.com/73020009/134882095-148b07ba-fd56-4669-a183-39dc80ed49a8.png)

In the reduce phase, given a cluster index we sum up all the related points and we compute a new centroid value.

![Schermata 2021-09-27 alle 11 25 22](https://user-images.githubusercontent.com/73020009/134882015-46c0dd7a-bcc0-453e-b5bf-80dfc29407b4.png)

These two steps are repeated until a certain number of iterations is reached.

## Hadoop Implementation
The code reported in this repository is an Hadoop implementation of the previously presented pseudo-code. The Hadoop driver is responsible to check the number of iterations and to update the centroids at the end of each stage, in order to guarantee the convergence of the algorithm.

## Experimental analysis
The code has been tested on two different datasets: the "iris dataset" (petalLenght-petalWidth) and a dataset of 1.000 points (in the range 0-100) generated from scratch with a python script. Both the datasets are composed by points in a two-dimensional space, in order to ease the visualization.

The "iris dataset" has been tested with 5 iterations using a k value equal to 2. The algorithm starts from 2 random centroids (yellow and orange points) and generate 2 new centroid at each iteration. As we can see from the image, as the number of iterations increases, the differences between subsequent centroids is minimized.

![iris](https://user-images.githubusercontent.com/73020009/134887759-af00103a-0322-44b1-b3a0-cd37605ab7cf.png)

The execution on the last dataset has been carried out with a number of 10 iterations and k=5.

![1000](https://user-images.githubusercontent.com/73020009/134888001-f667bba6-4fd6-43af-a11a-91ab7f4fb542.png)

