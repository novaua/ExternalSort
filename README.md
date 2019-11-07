# External Sort
Primer for generation of a huge file of random words and sorting using Merge Sort algorithm. The primer uses TPL multithreading and asynchronous IO where possible. This is not supper speedy yet, but has to work as expected.

Use folloiwng command to genearte big file.

PS L:\work\ExternalSort-master\bin\Debug> .\DataGenerator.exe
Generation file 1 MB
File 'out.txt' of total 34049 lines of size 1 MB generated
Test file generation of 1 MB took : 0.07 s, speed 13,62 MB/s
PS L:\work\ExternalSort-master\bin\Debug>

The output file looks like
1. hodge
2. psychodelic obj corodiastasis
3. iconoplast
4. conducive oversophistication supervastness
5. interchain
6. gallivanting toothchiseled
7. antinaturalistic
8. calory quauk
...
The format of the file is 
  <number>. <One or more words>
The words are taken from DataGenerator\Resources\words.txt which is an list of Eglish words found in the internet.

To sort the 'out.txt' file ExternalSort.exe may be used. It support '--help' which makes usage evident.

PS L:\work\ExternalSort-master\bin\Debug> .\ExternalSort.exe --help
Big files sorting tool
        Usage ExternalSort <input file> <output file> [/ord[inal]]
        Example:
        ExternalSort.exe in.txt outSorted.txt

Let's try it now.

PS L:\work\ExternalSort-master\bin\Debug> .\ExternalSort.exe .\out.txt .\sortedOut.txt
Read completeCreating temp files of 1 MB took : 3.48 s, speed 294,35 KB/s
1 files created. Total lines 34048. Max Queue size 8947848
Merge sort compressed files  of 464,48 KB took : 0.03 s, speed 13,01 MB/s
Merge sort input of 1 MB took : 0.03 s, speed 28,63 MB/s
Total work of 1 MB took : 3.56 s, speed 287,71 KB/s

Oh it works! If we look ito the output file it may look like
14261. 'em
11021. 'mid
20731. 'tween
16461. 've subilia cointer
33542. aardvarks undersomething photodermatic
31061. aaron suborns celomata
27504. aasvogel blindstory vanlay
12513. abactor trapezius
778. abaddon consociational encodement
27821. abamp axiate
13825. abandonable
13825. abandonable
15637. abandonee birdhouse
<...>

The output text in file now is sorted alphabetically first by the second column of words then by number or words id. 

