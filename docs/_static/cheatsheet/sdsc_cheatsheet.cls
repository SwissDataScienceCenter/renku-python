\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{sdsc_cheatsheet}[custom cheatsheet class]
\LoadClass{article}
\RequirePackage{titlesec}
\RequirePackage{multicol}
\RequirePackage{xcolor}
\RequirePackage{fontspec}
\RequirePackage{anyfontsize}
\RequirePackage{fancyhdr}
\RequirePackage{graphicx}
\RequirePackage{tikz}

\definecolor{title_underline}{cmyk}{100, 0, 75, 0}
\definecolor{footer_underline}{rgb}{0, 0.58, 0.41}
\definecolor{code_background}{gray}{0.9}

\newcommand{\definebackground}[1]{
  \AddToHook{shipout/background}
  {
    \put(0,-100)
    {
      \begin{tikzpicture}[remember picture, overlay]
        \node[opacity=0.9,anchor=north west] (One)
        {\includegraphics[height=\textheight]{#1}};
      \end{tikzpicture}%
    }
  }
}

\newcommand{\definefonts}[0]{
    \setmainfont[Path=./, Extension=.ttf, BoldFont=SourceSans3-Bold, ItalicFont=SourceSans3-Italic, BoldItalicFont=SourceSans3-BoldItalic, SmallCapsFeatures={Letters = SmallCaps}]{SourceSans3}
    \setmonofont[Path=./, Extension=.ttf, BoldFont=Inconsolata, ItalicFont=Inconsolata, BoldItalicFont=Inconsolata, SmallCapsFeatures={Letters = SmallCaps}]{Inconsolata}
}

\newcommand{\headersfooters}[1]{
    \setlength{\headheight}{50mm}

    \pagestyle{fancy}
    \fancyhf{}
    \renewcommand{\headrulewidth}{0pt}
    \fancyhead[L]{%
      \raisebox{-.25\height}{%
        \includegraphics[width=50mm]{RenkuLogo.pdf}\hspace{10mm}%
      }%
      \fontsize{40}{0}\selectfont \textbf{#1}%
    }

    \fancyfoot[C]{%
      %\noindent\makebox[\linewidth]{\color{footer_underline}\rule{18cm}{1pt}}%
      %\\
      \includegraphics[height=1cm]{ETHZ}%
      \hspace{20pt}%
      \raisebox{-1pt}{%
        \includegraphics[height=1cm]{SDSC}%
      }%
      \hspace{20pt}%
      \raisebox{-6pt}{%
        \includegraphics[height=1.3cm]{EPFL}%
      }%
    }
}

\newcounter {commandsubsection}

\titlespacing\section{0pt}{0pt plus 4pt minus 2pt}{0pt plus 2pt minus 2pt}

\titleformat{\section}         % Customise the \section command
  {\bfseries\scshape\centering\fontsize{20}{0}\selectfont} % Make the \section headers large (\Large),
                               % small capitals (\scshape) and left aligned (\raggedright)
  {}{0em}                      % Can be used to give a prefix to all sections, like 'Section ...'
  {\vspace{4pt}}                           % Can be used to insert code before the heading
  [\color{title_underline}\noindent{\vspace{-35pt}\rule[2em]{2ex}{6pt}}]                 % Inserts a horizontal line after the heading

\newcommand{\commandsubsection}[2]{
  \begin{samepage}
  \noindent\colorbox{code_background}{\parbox{\dimexpr\linewidth-2\fboxsep}{\texttt{\fontseries{bx}\selectfont#1}}}\vspace{2pt}
  \noindent\colorbox{white}{\parbox{\dimexpr\linewidth-2\fboxsep}{#2}}
  \end{samepage}
  \filbreak
  \vspace{2pt}
}