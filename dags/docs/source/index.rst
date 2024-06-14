.. snd-derivados documentation master file, created by
   sphinx-quickstart on Thu Jun  6 09:29:04 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Documentação 
=============

Esta página tem o intuito de registrar a documentação das regras de negócio desenvolvidas no projeto.

As regras de negócio aqui mencionadas são:

 - RN01
 - RN02
 - RN03

O projeto ******* foi desenvolvido pela equipe de Analytics LDT da Raízen e as modificações são de responsabilidade da mesma.
Sugestões de mudança e/ou correções devem ser encaminhadas para .... .


RN01 - Regra de Negócio 01
---------------------------

Exemplo de exibição de código :func:`bar`:

.. doctest::

  >>> from foo import bar
  >>> foo = bar()
  >>> foo is bar()
  True

Neste trecho deverá ser explicada a função apresentada acima, assim como todos os cálculos desenvolvidos. 
A explicação tem como interlocutor principal Analistas de Negócio que poderão retirar sua dúvidas sobre as regras a partir desta documentação.
Exemplos, quando aplicáveis, podem fazer parte desta documentação.


RN02 - Regra de Negócio 02
---------------------------

Trecho de documentação para explicar a segunda regra de negócio envolvida no projeto.
Seguirá a mesma estrutura apresentada na RN01.
Texto meramente ilustrativo


Documentação de código
-----------------------

Todas as funções desenvolvidas para o projeto deverão ser documentadas e publicadas aqui, conforme exemplo:

.. toctree::
   :numbered:
   :maxdepth: 4

   modules

.. include:: ../../../CHANGES

