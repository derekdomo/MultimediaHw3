function findPeak( from, to, type )
%UNTITLED Summary of this function goes here
%   Detailed explanation goes here
load('noiseWithSinusoid.mat');
val1=signal_with_sinusoid;
load('noiseWithMosquito.mat');
val2=signal_with_mosquito;
if type==1
    val1=val1(from:to);
    val1=fft(val1);
    val1=abs(val1);
    find(val1==max(val1))
end
if type==2
    val2=val2(from:to);
    val2=fft(val2);
    val2=abs(val2);
    find(val2==max(val2))
end
end

