Design Decisions:
During the course of the project's implementation, we meticulously architected a system to
facilitate the reading of tuples from a singular heap file. One significant design decision involved
the introduction of a private member, termed "pages". This member is essentially a vector of
HeapPage objects. This strategic addition grants us the capability to seamlessly navigate
through all tuples by harnessing the power of the iterator pattern. Specifically, leveraging
iterators from HeapPage empowers us to efficiently parse tuples and subsequently integrate
them within the HeapFile iterator, ensuring streamlined operations.
API Modifications:
Upon thorough evaluation of the provided Application Programming Interface (API), we
determined that no modifications or adjustments were essential for our implementation's
objectives. Thus, the API remains unaltered.
Code Completeness:
As per the stipulated requirements for PA1, we have successfully realized and executed all
essential components, ensuring a holistic solution.
Implementation Duration and Challenges:
The entire duration of PA1's implementation spanned approximately one full week. Throughout
this period, we diligently committed changes to our GitHub repository over 30 times, ensuring
version control best practices and tracking our progress.
The journey was not devoid of challenges. We encountered complexities while interfacing with
the SeqScan component. Its intricate design, which encompasses references to multiple
iterators – namely the HeapFile iterator, HeapPage iterator, and Tuple iterator, presented certain
ambiguities. Furthermore, complications arose during the generation of heapfile.dat. Repeated
instances of segmentation faults were triggered during attempts to read data from this file.
However, after rigorous debugging and alterations to our runtime configuration, we triumphed
over this impediment.
Collaboration Details:
Our team employed a collaborative approach to tackle the project. Specifically:
Yanpeng Zhao spearheaded the tasks encompassed in EX1.1, 1.2, and 1.3.
Pengfei Li undertook the challenges presented in EX1.4, 1.5, and 1.6.
While each member primarily focused on their designated segments, our collaboration wasn't
confined strictly to these demarcations. We embraced collective brainstorming, debugging, and
testing, ensuring that the final output was a product of combined expertise and team synergy.