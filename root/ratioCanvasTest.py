#!/usr/bin/env python
from ROOT import TH1D,gROOT
gROOT.SetBatch(True)
from RatioCanvas import RatioCanvas
from AtlasStyle import SetAtlasStyle
SetAtlasStyle()

ratio = RatioCanvas(
                     ratioPad_y_ndiv      = 204,
                   )

h1 = TH1D('h1',';p_{T} [GeV];Counts',150,0,150)
h1.FillRandom('landau',100000)
h1.Sumw2()
h2 = TH1D('h2',';p_{T} [GeV];Counts',150,0,150)
h2.FillRandom('landau',100000)
h2.Sumw2()
h3 = TH1D('h3',';p_{T} [GeV];Counts',150,0,150)
h3.FillRandom('landau',100000)
h3.Sumw2()

h1.GetXaxis().SetTitleOffset(1)

ratio.AddPlot(h1,'e','first','lp',True)
ratio.AddPlot(h2,'e','second','lp')
ratio.AddPlot(h3,'e','third','lp')

ratio.Draw('ratioTest.ps')

   
