function plotMos()
load('noiseWithMosquito.mat');
val=signal_with_mosquito;
len=length(val)/2;
mat=zeros(1,length(val));
iter=1;
while len>1
    diff=zeros(1,len);
    smooth=zeros(1,len);
    for i=1:len
        diff(i)=(val(2*i-1)-val(2*i))/sqrt(2);
        smooth(i)=(val(2*i-1)+val(2*i))/sqrt(2);
    end
    mat=[mat;kron(diff,ones(1,2^iter))];
    val=smooth;
    len=round(len/2);
    iter=iter+1;
end
colormap(pink(12));
image(mat);
saveas(gcf,'Mos.jpg');
end