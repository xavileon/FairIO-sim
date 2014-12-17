FairIO controller simulator

This is the simulator used for the FairIO paper submitted to ICDCS'15. To get an idea of the scope of this work, here is the abstract:

Big Data analytics systems are widely employed by companies to extract valuable business information from vasts amounts of data generated through their day to day operation. In this context, Infrastructure providers are trading their computational and storage infrastructure to other enterprise clients or tenants by offering analytics as a service. However, most Big Data analytics systems today, e.g. the Hadoop ecosystem, provide weak or no I/O storage bandwidth differentiation among tenants and the data they access. Current approaches to multi-tenant or multi-user resource allocation in these frameworks are based on fair sharing of computational slots, and I/O bandwidth differentiation has not been properly studied in depth because of data locality constraints, i.e. computing over local data was usually faster than over remote data. Because of this constraint, complex mechanisms involving locality-aware computation and replication strategies were necessary. However, the advent of new datacenter network technologies which provides near full-bisection bandwidth can render this constraint obsolete. Under this assumption, we propose FairIO, a combination of optimal storage I/O bandwidth allocation to each replica at each disk and a flexible replication strategy, to achieve per-tenant and per-file weighted fair sharing  of the total datacenter storage bandwidth available.

Requirements:

Python packages:
- simpy (simulation package)
- bitarray (bitwise operations)
- cDecimal (arbitrary precission decimal operations)

Installation:
- Install required packages
- execute (--help for explanation): python iosumulator.py
