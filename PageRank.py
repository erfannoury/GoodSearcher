import numpy as np
import scipy as sc


def Normalize(adjmat, tele_const = 0.2):
    """
    This method will try and normalize the adjacency matrix, so that it will be suitable for the PageRank algorithm. Using the teleporting constant it will remove the effect of deadends in the PageRank algorithm.

    Parameters
    ----------
    adjmat: numpy array
            a square adjacency matrix
    tele_const: float
                teleporting constant for the PageRank algorithm (P' = (1-alpha)*P + alpha*v)

    Returns
    -------
    mat: numpy array
         an square matrix of size equal to the adjmat matrix and normalized
    """

    mat = np.zeros(adjmat.shape)
    cols = adjmat.shape[0]
    deadend_const = 1.0 / cols
    for i in range(cols):
        s = np.sum(adjmat[i,:])
        if s == 0:
            mat[i,:] = deadend_const
        else:
            mat[i,:] = adjmat[i,:] / s
        mat[i,:] = mat[i,:] * (1 - tele_const) + deadend_const * tele_const
    return mat

def PageRankScores(norm_mat):
    """
    Calculates the PageRank score vector for norm_mat normalized adjacency matrix

    Parameters
    ----------
    norm_mat: numpy array
              the normalized adjacency matrix, such that each row sums up to one.

    Returns
    -------
    sv: numpy array
        the PageRank score vector
    """
    sw, sv = sc.sparse.linalg.eigs(norm_mat.T, k=1, which='LR')
    sv = sv.T[0]
    sv = np.abs(sv)
    sv /= np.sum(sv)
    return sv
