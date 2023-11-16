package main

import "key-value-engine/structs/simHash"

func main() {
	s1 := simHash.SimHash([]byte(`
Like all forests, the wooded stretches of the Arctic sometimes catch on fire. 
But unlike many forests in the mid-latitudes, which thrive on or even require fire to preserve their health, 
Arctic forests have evolved to burn only infrequently. Climate change is reshaping that regime. 
In the first decade of the new millennium, fires burned 50 percent more acreage each year in the Arctic, 
on average, than any decade in the 1900s. Between 2010 and 2020, burned acreage continued to creep up, 
particularly in Alaska, which had its second-worst fire year ever in 2015 and another bad one in 2019. 
Scientists have found that fire frequency today is higher than at any time since the formation of boreal forests some 3,000 years ago, 
and potentially higher than at any point in the last 10,000 years.
Fires in boreal forests can release even more carbon than similar fires in places like California or Europe, 
because the soils underlying the high-latitude forests are often made of old, carbon-rich peat. 
In 2020, Arctic fires released almost 250 megatons of carbon dioxide, about half as much as Australia emits in a year from human activities 
and about 2.5 times as much as the record-breaking 2020 California wildfire season.
`))
	s2 := simHash.SimHash([]byte(`
The Amazon rainforest is most likely now a net contributor to warming of the planet, 
according to a first-of-its-kind analysis from more than 30 scientists. 
For years, researchers have expressed concern that rising temperatures, drought, 
and deforestation are reducing the capacity of the world’s largest rainforest 
to absorb carbon dioxide from the atmosphere, and help offset emissions from fossil-fuel burning. 
Recent studies have even suggested that some portions of the tropical landscape already may release 
more carbon than they store. 
But the inhaling and exhaling of CO2 is just one way this damp jungle, 
the most species-rich on Earth, influences the global climate. 
Activities in the Amazon, both natural and human-caused, can shift the rainforest’s contribution 
in significant ways, warming the air directly or releasing other greenhouse gases that do. 
Drying wetlands and soil compaction from logging, for example, can increase emissions of the greenhouse gas nitrous oxide. 
Land-clearing fires release black carbon, small particles of soot that absorb sunlight and increase warmth. 
Deforestation can alter rainfall patterns, further drying and heating the forest. 
Regular flooding and dam-building releases the potent gas methane, as does cattle ranching, 
one chief reason forests are destroyed. 
And roughly 3.5 percent of all methane released globally comes naturally from the Amazon’s trees.
`))

	println(simHash.HemingDistance(s1, s2))

	println(simHash.HemingDistance(simHash.SimHash([]byte("tekst koji probavam")),
		simHash.SimHash([]byte("koja probavam tekst"))))

}
