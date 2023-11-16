package cms

import (
	"math"
)

/*
CalculateM determines the number of counters per hash function for a Count-Min Sketch (CMS) based on the privacy parameter epsilon.

Parameters:
  - epsilon: A floating-point value influencing the accuracy of the CMS. Smaller values result in more accurate estimations.

Returns:
  - Number of counters per hash function (m) calculated based on the provided epsilon.

Note: The CalculateM function is used to determine the optimal number of counters per hash function for a given parameter.
*/
func calculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

/*
CalculateK determines the number of hash functions for a Count-Min Sketch (CMS) based on the privacy parameter delta.

Parameters:
  - delta: A floating-point value controlling the failure probability of the CMS. Smaller delta reduces the likelihood of overestimation.

Returns:
  - Number of hash functions (k) calculated based on the provided delta.

Note: The CalculateK function is used to determine the optimal number of hash functions for a given privacy parameter, influencing the failure probability of the CMS.
*/
func calculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(1 / delta)))
}
