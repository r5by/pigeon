# How to use Pigeon simulator

## File Structure
The Pigeon simulator includes 9 files
+ pigeon.cpp       -----the main program of pigeon.
+ job.h            -----the parameter set up head file
+ WORK.h           -----the class of a workerMASTER.h         -----the class of a master
+ DIST.h           -----the class of a distributor
+ global.h         -----the simulator setup head file
+ List.h           -----the class of list
+ List.cpp         -----the functions of list
+ cal_result.cpp   -----the file used to calculate the result, given 50, 90 and 99th percentile

## Instructions
The following instructions are on how to complile and run the simulator, different systems may be different.
The above 8 files can be complied as 
```bash
g++ pigeon.cpp -lm -o pigeon
```
pigeon is a complied file, can be run as `./pigeon`

Using cal_result.cpp compile: 
```bash
g++ cal_result.cpp -lm -o cal
```
run: `./cal`


## Contact
- Zhijun Wang <zhijun.wang@uta.edu>
