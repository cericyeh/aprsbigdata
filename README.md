https://github.com/aThis contains code put together for Bill Howe's Introduction to Data Science Course, from University of Washington, on Coursera.

The main intent of this code is to identify digipeater coverage for an area, and for this task we do this in the simplest way possible: we only identify direct over-the-air first hop receipts.  We currenlty do not attempt to deal with any RF propagation models, nor do we take elevation nor power into account.  Instead, we simply aggregate and display receipts by position, as well as using some simple histograms for indicating quality of reception.

This code consists of two phases:

- A Hadoop MapReduce job for processing timestampped APRS packets, identifying only packets that were one-hop over the air receipts.

- Standalone Scala classes for identifying and visualizing packets heard by a specific set of iGates, and/or those within a given bounding box.

This makes use of the javAPRSlib library (https://github.com/ab0oo/javAPRSlib) for processing APRS packets and translating their positional components.  As with jaAPRSlib, this code is LGPL.

The code is currently very rough, due to the compressed timeline for the class project, and will be subject to further cleanup.  There may be some munging of terminology as well, due to the author's lack of familiarity with APRS.  

Eric Yeh, KJ6UHP

Acknowledgements

The author would like to thank Alan Crosswell for putting together the dataset, motivating the problem, and providing comments and suggestions.