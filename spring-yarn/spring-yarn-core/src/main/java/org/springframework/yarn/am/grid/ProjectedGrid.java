package org.springframework.yarn.am.grid;

import java.util.Collection;

import org.springframework.yarn.am.grid.listener.ProjectedGridListener;

/**
 * ProjectedGrid
 *
 * @author Janne Valkealahti
 * @see Grid
 */
public interface ProjectedGrid {

	/**
	 * Adds a new grid projection.
	 * <p>
     * If a projected grid refuses to add a particular grid projection for any reason
     * other than that it already contains the grid projection, it <i>must</i> throw
     * an exception (rather than returning <tt>false</tt>).  This preserves
     * the invariant that a projected grid always contains the specified grid projection
     * after this call returns.
	 *
	 * @param projection the grid projection
	 * @return <tt>true</tt> if this projected grid changed as a result of the call
	 */
	boolean addProjection(GridProjection projection);

	/**
	 * Removes a grid projection.
	 * <p>
     * Removes a single instance of the specified grid projection from this
     * projected grid, if it is present.
	 *
	 * @param projection the grid projection
	 * @return <tt>true</tt> if a grid projection was removed as a result of this call
	 */
	boolean removeProjection(GridProjection projection);

	/**
	 * Gets a collection of grid projections or empty collection of there
	 * are no projections defined.
	 *
	 * @return the collection of grid projections
	 */
	Collection<GridProjection> getProjections();

	/**
	 * Adds a listener to be notified of projected grid events.
	 *
	 * @param listener the projected grid listener
	 */
	void addProjectedGridListener(ProjectedGridListener listener);

}
