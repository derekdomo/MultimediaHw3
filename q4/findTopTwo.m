function [res]=findTopTwo()
%UNTITLED2 Summary of this function goes here
%   Detailed explanation goes here
    Files = dir(strcat('./Train/','*.wav'));
    load('./Train/classLabel.mat');
    lengthFiles = length(Files);
    res=zeros(lengthFiles,2);
    for i=1:lengthFiles
        data=audioread(strcat('./Train/',Files(i).name));
        [x,y]=findTwoPeak(data);
        res(1,i)=x;
        res(2,i)=y;
    end
    scatter(res(1,:),res(2,:),25,labels(:,2),'filled');
    saveas(gcf,'scatter.png');
end

function [x,y] = findTwoPeak(data)
    res=fft(data);
    res=res(100:round(length(res)/2));
    res=abs(res);
    [pks,locs] = findpeaks(res,'MinPeakDistance',100);
    [t,i]=sort(pks,'descend');
    if length(i)==1
        x=locs(1);
        y=0;
    end
    if length(i)==2
        x=locs(i(1));
        y=locs(i(2));
    end
    if length(i)==0
        x=0;
        y=0;
    end
    if length(i)>2
        f1=locs(i(1));
        f2=locs(i(2));
        f3=locs(i(3));
        f=sort([f1,f2,f3]);
        x=f(1);
        y=f(2);
    end
end

